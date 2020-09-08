/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
 * the License. A copy of the License is located at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

namespace Amazon.QLDB.Driver.Tests
{
    using System;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging.Abstractions;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class SessionPoolTests
    {
        [TestMethod]
        public void Constructor_CreateSessionPool_NewSessionPoolCreated()
        {
            Assert.IsNotNull(new SessionPool(() => Task.FromResult(new Mock<Session>(null, null, null, null, null).Object),
                new Mock<IRetryHandler>().Object, 1, NullLogger.Instance));
        }

        [TestMethod]
        public void GetSession_GetSessionFromPool_NewSessionReturned()
        {
            var mockCreator = new Mock<Func<Task<Session>>>();
            var mockSession = new Mock<Session>(null, null, null, null, null).Object;
            mockCreator.Setup(x => x()).ReturnsAsync(mockSession);

            var pool = new SessionPool(mockCreator.Object, QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance), 1, NullLogger.Instance);
            var returnedSession = pool.GetSession();

            Assert.IsNotNull(returnedSession);
            mockCreator.Verify(x => x(), Times.Exactly(1));
        }

        [TestMethod]
        public async Task GetSession_GetSessionFromPool_ExpectedSessionReturned()
        {
            var mockCreator = new Mock<Func<Task<Session>>>();
            var session = new Session(null, null, null, "testSessionId", null);
            mockCreator.Setup(x => x()).ReturnsAsync(session);

            var pool = new SessionPool(mockCreator.Object, QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance), 1, NullLogger.Instance);
            var returnedSession = await pool.GetSession();

            Assert.AreEqual(session.SessionId, returnedSession.GetSessionId());
        }

        [TestMethod]
        public async Task GetSession_GetTwoSessionsFromPoolOfOne_TimeoutOnSecondGet()
        {
            var mockCreator = new Mock<Func<Task<Session>>>();
            var mockSession = new Mock<Session>(null, null, null, null, null).Object;
            mockCreator.Setup(x => x()).ReturnsAsync(mockSession);

            var pool = new SessionPool(mockCreator.Object, QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance), 1, NullLogger.Instance);
            var returnedSession = pool.GetSession();
            await Assert.ThrowsExceptionAsync<QldbDriverException>(() => pool.GetSession());

            Assert.IsNotNull(returnedSession);
            mockCreator.Verify(x => x(), Times.Exactly(1));
        }

        [TestMethod]
        public async Task GetSession_GetTwoSessionsFromPoolOfOneAfterFirstOneDisposed_NoThrowOnSecondGet()
        {
            var mockCreator = new Mock<Func<Task<Session>>>();
            var mockSession = new Mock<Session>(null, null, null, null, null);
            mockCreator.Setup(x => x()).ReturnsAsync(mockSession.Object);
            mockSession.Setup(x => x.StartTransaction(It.IsAny<CancellationToken>())).ReturnsAsync(new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            });

            var pool = new SessionPool(mockCreator.Object, QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance), 1, NullLogger.Instance);
            var returnedSession = await pool.GetSession();
            returnedSession.Release();
            var nextSession = await pool.GetSession();
            Assert.IsNotNull(nextSession);

            await nextSession.StartTransaction();
            mockCreator.Verify(x => x(), Times.Exactly(1));
        }

        [TestMethod]
        public async Task GetSession_FailedToCreateSession_ThrowTheOriginalException()
        {
            var mockCreator = new Mock<Func<Task<Session>>>();
            var exception = new AmazonServiceException("test");
            mockCreator.Setup(x => x()).ThrowsAsync(exception);

            var pool = new SessionPool(mockCreator.Object, QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance), 1, NullLogger.Instance);

            await Assert.ThrowsExceptionAsync<AmazonServiceException>(() => pool.GetSession());
        }

        [TestMethod]
        public async Task GetSession_DisposeSession_ShouldNotEndSession()
        {
            var mockCreator = new Mock<Func<Task<Session>>>();
            var mockSession = new Mock<Session>(null, null, null, null, null);
            mockCreator.Setup(x => x()).ReturnsAsync(mockSession.Object);

            var pool = new SessionPool(mockCreator.Object, QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance), 1, NullLogger.Instance);

            var returnedSession = await pool.GetSession();

            returnedSession.Release();

            mockSession.Verify(s => s.End(It.IsAny<CancellationToken>()), Times.Exactly(0));
        }

        [TestMethod]
        public async Task Execute_NoException_ReturnFunctionValue()
        {
            var mockCreator = new Mock<Func<Task<Session>>>();
            var mockSession = new Mock<Session>(null, null, null, null, null);
            mockCreator.Setup(x => x()).ReturnsAsync(mockSession.Object);
            var retry = new Mock<Func<int, Task>>();

            var sendCommandResponseStart = new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            };

            var h1 = QldbHash.ToQldbHash("testTransactionIdddddd");

            var sendCommandResponseCommit = new CommitTransactionResult
            {
                CommitDigest = new MemoryStream(h1.Hash),
                TransactionId = "testTransactionIdddddd"
            };

            mockSession.Setup(x => x.StartTransaction(It.IsAny<CancellationToken>()))
                .ReturnsAsync(sendCommandResponseStart);
            mockSession.Setup(x => x.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(sendCommandResponseCommit);

            var mockFunction = new Mock<Func<TransactionExecutor, Task<int>>>();
            mockFunction.Setup(f => f.Invoke(It.IsAny<TransactionExecutor>())).ReturnsAsync(1);
            var mockRetry = new Mock<Action<int>>();

            var pool = new SessionPool(mockCreator.Object, QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance), 1, NullLogger.Instance);

            await pool.Execute(mockFunction.Object, Driver.RetryPolicy.Builder().Build(), retry.Object);

            mockCreator.Verify(x => x(), Times.Once);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Never);
        }

        [TestMethod]
        public async Task Execute_HaveOCCExceptionsWithinRetryLimit_Succeeded()
        {
            var mockCreator = new Mock<Func<Task<Session>>>();
            var mockSession = new Mock<Session>(null, null, null, null, null);
            mockCreator.Setup(x => x()).ReturnsAsync(mockSession.Object);
            var retry = new Mock<Func<int, Task>>();

            var sendCommandResponseStart = new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            };

            var h1 = QldbHash.ToQldbHash("testTransactionIdddddd");

            var sendCommandResponseCommit = new CommitTransactionResult
            {
                CommitDigest = new MemoryStream(h1.Hash),
                TransactionId = "testTransactionIdddddd"
            };

            mockSession.Setup(x => x.StartTransaction(It.IsAny<CancellationToken>()))
                .ReturnsAsync(sendCommandResponseStart);
            mockSession.Setup(x => x.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(sendCommandResponseCommit);

            var mockFunction = new Mock<Func<TransactionExecutor, Task<int>>>();
            mockFunction.SetupSequence(f => f.Invoke(It.IsAny<TransactionExecutor>()))
                .ThrowsAsync(new OccConflictException("occ"))
                .ThrowsAsync(new OccConflictException("occ"))
                .ThrowsAsync(new OccConflictException("occ"))
                .ReturnsAsync(1);
            var mockRetry = new Mock<Action<int>>();

            var pool = new SessionPool(mockCreator.Object, QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance), 1, NullLogger.Instance);

            await pool.Execute(mockFunction.Object, Driver.RetryPolicy.Builder().Build(), retry.Object);

            mockCreator.Verify(x => x(), Times.Once);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Exactly(3));
            Assert.AreEqual(1, pool.AvailablePermit());
        }

        [TestMethod]
        public async Task Execute_HaveOCCExceptionsAndAbortFailuresWithinRetryLimit_Succeeded()
        {
            var mockCreator = new Mock<Func<Task<Session>>>();
            var mockSession = new Mock<Session>(null, null, null, null, null);
            mockCreator.Setup(x => x()).ReturnsAsync(mockSession.Object);
            var retry = new Mock<Func<int, Task>>();

            var sendCommandResponseStart = new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            };

            var h1 = QldbHash.ToQldbHash("testTransactionIdddddd");

            var sendCommandResponseCommit = new CommitTransactionResult
            {
                CommitDigest = new MemoryStream(h1.Hash),
                TransactionId = "testTransactionIdddddd"
            };

            var abortResponse = new AbortTransactionResult { };
            var serviceException = new AmazonServiceException();

            mockSession.Setup(x => x.StartTransaction(It.IsAny<CancellationToken>()))
                .ReturnsAsync(sendCommandResponseStart);
            mockSession.Setup(x => x.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(sendCommandResponseCommit);
            mockSession.SetupSequence(x => x.AbortTransaction(It.IsAny<CancellationToken>()))
                .ThrowsAsync(serviceException)
                .ReturnsAsync(abortResponse)
                .ThrowsAsync(serviceException);

            var mockFunction = new Mock<Func<TransactionExecutor, Task<int>>>();
            mockFunction.SetupSequence(f => f.Invoke(It.IsAny<TransactionExecutor>()))
                .ThrowsAsync(new OccConflictException("occ"))
                .ThrowsAsync(new OccConflictException("occ"))
                .ThrowsAsync(new OccConflictException("occ"))
                .ReturnsAsync(1);
            var mockRetry = new Mock<Action<int>>();

            var pool = new SessionPool(mockCreator.Object, QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance), 2, NullLogger.Instance);

            var session1 = await pool.GetSession();
            var session2 = await pool.GetSession();
            session1.Release();
            session2.Release();

            await pool.Execute(mockFunction.Object, Driver.RetryPolicy.Builder().Build(), retry.Object);

            mockCreator.Verify(x => x(), Times.Exactly(2));
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Exactly(3));
            Assert.AreEqual(2, pool.AvailablePermit());
        }

        [TestMethod]
        public async Task Execute_HaveOCCExceptionsAboveRetryLimit_ThrowOCC()
        {
            var mockCreator = new Mock<Func<Task<Session>>>();
            var mockSession = new Mock<Session>(null, null, null, null, null);
            mockCreator.Setup(x => x()).ReturnsAsync(mockSession.Object);
            var retry = new Mock<Func<int, Task>>();

            var sendCommandResponseStart = new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            };

            var h1 = QldbHash.ToQldbHash("testTransactionIdddddd");

            var sendCommandResponseCommit = new CommitTransactionResult
            {
                CommitDigest = new MemoryStream(h1.Hash),
                TransactionId = "testTransactionIdddddd"
            };

            mockSession.Setup(x => x.StartTransaction(It.IsAny<CancellationToken>()))
                .ReturnsAsync(sendCommandResponseStart);
            mockSession.Setup(x => x.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(sendCommandResponseCommit);

            var mockFunction = new Mock<Func<TransactionExecutor, Task<int>>>();
            mockFunction.SetupSequence(f => f.Invoke(It.IsAny<TransactionExecutor>()))
                .ThrowsAsync(new OccConflictException("occ"))
                .ThrowsAsync(new OccConflictException("occ"))
                .ThrowsAsync(new OccConflictException("occ"))
                .ThrowsAsync(new OccConflictException("occ"))
                .ThrowsAsync(new OccConflictException("occ"));
            var mockRetry = new Mock<Action<int>>();

            var pool = new SessionPool(mockCreator.Object, QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance), 1, NullLogger.Instance);

            await Assert.ThrowsExceptionAsync<OccConflictException>(() => pool.Execute(mockFunction.Object, Driver.RetryPolicy.Builder().Build(), retry.Object));

            mockCreator.Verify(x => x(), Times.Once);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Exactly(4));
        }

        [TestMethod]
        public async Task Execute_HaveISE_Succeeded()
        {
            var mockCreator = new Mock<Func<Task<Session>>>();
            var mockSession = new Mock<Session>(null, null, null, null, null);
            mockCreator.Setup(x => x()).ReturnsAsync(mockSession.Object);
            var retry = new Mock<Func<int, Task>>();

            var sendCommandResponseStart = new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            };

            var h1 = QldbHash.ToQldbHash("testTransactionIdddddd");

            var sendCommandResponseCommit = new CommitTransactionResult
            {
                CommitDigest = new MemoryStream(h1.Hash),
                TransactionId = "testTransactionIdddddd"
            };

            mockSession.Setup(x => x.StartTransaction(It.IsAny<CancellationToken>()))
                .ReturnsAsync(sendCommandResponseStart);
            mockSession.Setup(x => x.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(sendCommandResponseCommit);

            var mockFunction = new Mock<Func<TransactionExecutor, Task<int>>>();
            mockFunction.SetupSequence(f => f.Invoke(It.IsAny<TransactionExecutor>()))
                .ThrowsAsync(new InvalidSessionException("invalid"))
                .ThrowsAsync(new InvalidSessionException("invalid"))
                .ThrowsAsync(new InvalidSessionException("invalid"))
                .ThrowsAsync(new InvalidSessionException("invalid"))
                .ReturnsAsync(1);
            var mockRetry = new Mock<Action<int>>();

            var pool = new SessionPool(mockCreator.Object, QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance), 2, NullLogger.Instance);

            var session1 = await pool.GetSession();
            var session2 = await pool.GetSession();
            session1.Release();
            session2.Release();

            await pool.Execute(mockFunction.Object, Driver.RetryPolicy.Builder().Build(), retry.Object);

            mockCreator.Verify(x => x(), Times.Exactly(6));
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Exactly(4));
            Assert.AreEqual(2, pool.AvailablePermit());
        }

        [TestMethod]
        public async Task Dispose_DisposeSessionPool_DestroyAllSessions()
        {
            var mockCreator = new Mock<Func<Task<Session>>>();
            var mockSession1 = new Mock<Session>(null, null, null, null, null);
            var mockSession2 = new Mock<Session>(null, null, null, null, null);
            mockCreator.SetupSequence(x => x()).ReturnsAsync(mockSession1.Object).ReturnsAsync(mockSession2.Object);

            var pool = new SessionPool(mockCreator.Object, QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance), 2, NullLogger.Instance);

            var session1 = await pool.GetSession();
            var session2 = await pool.GetSession();
            session1.Release();
            session2.Release();

            await pool.DisposeAsync();

            mockSession1.Verify(s => s.End(It.IsAny<CancellationToken>()), Times.Exactly(1));
            mockSession2.Verify(s => s.End(It.IsAny<CancellationToken>()), Times.Exactly(1));
        }
    }
}
