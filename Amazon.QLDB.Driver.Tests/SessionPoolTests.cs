/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging.Abstractions;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class SessionPoolTests
    {
        private static readonly Mock<IRetryHandler> mockRetryHandler = new Mock<IRetryHandler>();
        private static readonly IRetryHandler retryHandler = QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance);
        private static Mock<Action<int>> retry;
        private static Mock<Func<TransactionExecutor, int>> mockFunction;
        private static Mock<Func<Session>> mockCreator;
        private static Mock<Session> mockSession;
        private static StartTransactionResult sendCommandResponseStart;
        private static CommitTransactionResult sendCommandResponseCommit;

        [TestInitialize]
        public void Setup()
        {
            mockCreator = new Mock<Func<Session>>();
            mockSession = new Mock<Session>(null, null, null, null, null);
            mockCreator.Setup(f => f()).Returns(mockSession.Object);

            retry = new Mock<Action<int>>();

            sendCommandResponseStart = new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            };

            var h1 = QldbHash.ToQldbHash("testTransactionIdddddd");

            sendCommandResponseCommit = new CommitTransactionResult
            {
                CommitDigest = new MemoryStream(h1.Hash),
                TransactionId = "testTransactionIdddddd"
            };

            mockSession.Setup(x => x.StartTransaction()).Returns(sendCommandResponseStart);
            mockSession.Setup(x => x.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>()))
                .Returns(sendCommandResponseCommit);

            mockFunction = new Mock<Func<TransactionExecutor, int>>();
        }

        [TestMethod]
        public void Constructor_CreateSessionPool_NewSessionPoolCreated()
        {
            Assert.IsNotNull(getSessionPool(mockRetryHandler.Object, 1));
        }

        [TestMethod]
        public void GetSession_GetSessionFromPool_NewSessionReturned()
        {
            var pool = getSessionPool(retryHandler, 1);
            var returnedSession = pool.GetSession();

            Assert.IsNotNull(returnedSession);
            mockCreator.Verify(x => x(), Times.Exactly(1));
        }

        [TestMethod]
        public void GetSession_GetSessionFromPool_ExpectedSessionReturned()
        {
            var session = new Session(null, null, null, "testSessionId", null);
            mockCreator.Setup(x => x()).Returns(session);

            var pool = getSessionPool(retryHandler, 1);
            var returnedSession = pool.GetSession();

            Assert.AreEqual(session.SessionId, returnedSession.GetSessionId());
        }

        [TestMethod]
        public void GetSession_GetTwoSessionsFromPoolOfOne_TimeoutOnSecondGet()
        {
            var pool = getSessionPool(retryHandler, 1);
            var returnedSession = pool.GetSession();
            Assert.ThrowsException<QldbDriverException>(() => pool.GetSession());

            Assert.IsNotNull(returnedSession);
            mockCreator.Verify(x => x(), Times.Exactly(1));
        }

        [TestMethod]
        public void GetSession_GetTwoSessionsFromPoolOfOneAfterFirstOneDisposed_NoThrowOnSecondGet()
        {
            var pool = getSessionPool(retryHandler, 1);
            var returnedSession = pool.GetSession();
            returnedSession.Release();
            var nextSession = pool.GetSession();
            Assert.IsNotNull(nextSession);

            nextSession.StartTransaction();
            mockCreator.Verify(x => x(), Times.Exactly(1));
        }

        [TestMethod]
        public void GetSession_FailedToCreateSession_ThrowTheOriginalException()
        {
            var exception = new AmazonServiceException("test");
            mockCreator.Setup(x => x()).Throws(exception);

            var pool = getSessionPool(retryHandler, 1);

            Assert.ThrowsException<AmazonServiceException>(() => pool.GetSession());
        }

        [TestMethod]
        public void GetSession_DisposeSession_ShouldNotEndSession()
        {
            var pool = getSessionPool(retryHandler, 1);

            var returnedSession = pool.GetSession();

            returnedSession.Release();

            mockSession.Verify(s => s.End(), Times.Exactly(0));
        }

        [TestMethod]
        public void Execute_NoException_ReturnFunctionValue()
        {
            mockFunction.Setup(f => f.Invoke(It.IsAny<TransactionExecutor>())).Returns(1);

            var pool = getSessionPool(retryHandler, 1);

            pool.Execute(mockFunction.Object, Driver.RetryPolicy.Builder().Build(), retry.Object);

            mockCreator.Verify(x => x(), Times.Once);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Never);
        }

        [TestMethod]
        public void Execute_HaveOCCExceptionsWithinRetryLimit_Succeeded()
        {
            var mockFunction = new Mock<Func<TransactionExecutor, int>>();
            mockFunction.SetupSequence(f => f.Invoke(It.IsAny<TransactionExecutor>()))
                .Throws(new OccConflictException("occ"))
                .Throws(new OccConflictException("occ"))
                .Throws(new OccConflictException("occ"))
                .Returns(1);

            var pool = getSessionPool(retryHandler, 1);

            pool.Execute(mockFunction.Object, Driver.RetryPolicy.Builder().Build(), retry.Object);

            mockCreator.Verify(x => x(), Times.Once);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Exactly(3));
            Assert.AreEqual(1, pool.AvailablePermit());
        }

        [TestMethod]
        public void Execute_HaveOCCExceptionsAndAbortFailuresWithinRetryLimit_Succeeded()
        {
            var abortResponse = new AbortTransactionResult { };
            var serviceException = new AmazonServiceException();

            mockSession.SetupSequence(x => x.AbortTransaction())
                .Throws(serviceException)
                .Returns(abortResponse)
                .Throws(serviceException);

            var mockFunction = new Mock<Func<TransactionExecutor, int>>();
            mockFunction.SetupSequence(f => f.Invoke(It.IsAny<TransactionExecutor>()))
                .Throws(new OccConflictException("occ"))
                .Throws(new OccConflictException("occ"))
                .Throws(new OccConflictException("occ"))
                .Returns(1);

            var pool = getSessionPool(retryHandler, 2);

            var session1 = pool.GetSession();
            var session2 = pool.GetSession();
            session1.Release();
            session2.Release();

            pool.Execute(mockFunction.Object, Driver.RetryPolicy.Builder().Build(), retry.Object);

            mockCreator.Verify(x => x(), Times.Exactly(2));
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Exactly(3));
            Assert.AreEqual(2, pool.AvailablePermit());
        }

        [TestMethod]
        public void Execute_HaveOCCExceptionsAboveRetryLimit_ThrowOCC()
        {
            var mockFunction = new Mock<Func<TransactionExecutor, int>>();
            mockFunction.SetupSequence(f => f.Invoke(It.IsAny<TransactionExecutor>()))
                .Throws(new OccConflictException("occ"))
                .Throws(new OccConflictException("occ"))
                .Throws(new OccConflictException("occ"))
                .Throws(new OccConflictException("occ"))
                .Throws(new OccConflictException("occ"));

            var pool = getSessionPool(retryHandler, 1);

            Assert.ThrowsException<OccConflictException>(() => pool.Execute(mockFunction.Object, Driver.RetryPolicy.Builder().Build(), retry.Object));

            mockCreator.Verify(x => x(), Times.Once);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Exactly(4));
        }

        [TestMethod]
        public void Execute_HaveISE_Succeeded()
        {
            var mockFunction = new Mock<Func<TransactionExecutor, int>>();
            mockFunction.SetupSequence(f => f.Invoke(It.IsAny<TransactionExecutor>()))
                .Throws(new InvalidSessionException("invalid"))
                .Throws(new InvalidSessionException("invalid"))
                .Throws(new InvalidSessionException("invalid"))
                .Throws(new InvalidSessionException("invalid"))
                .Returns(1);

            var pool = getSessionPool(retryHandler, 2);

            var session1 = pool.GetSession();
            var session2 = pool.GetSession();
            session1.Release();
            session2.Release();

            pool.Execute(mockFunction.Object, Driver.RetryPolicy.Builder().Build(), retry.Object);

            mockCreator.Verify(x => x(), Times.Exactly(6));
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Exactly(4));
            Assert.AreEqual(2, pool.AvailablePermit());
        }

        [TestMethod]
        public void Dispose_DisposeSessionPool_DestroyAllSessions()
        {
            var mockSession1 = new Mock<Session>(null, null, null, null, null);
            var mockSession2 = new Mock<Session>(null, null, null, null, null);
            mockCreator.SetupSequence(x => x()).Returns(mockSession1.Object).Returns(mockSession2.Object);

            var pool = getSessionPool(retryHandler, 2);

            var session1 = pool.GetSession();
            var session2 = pool.GetSession();
            session1.Release();
            session2.Release();

            pool.Dispose();

            mockSession1.Verify(s => s.End(), Times.Exactly(1));
            mockSession2.Verify(s => s.End(), Times.Exactly(1));
        }

        private static SessionPool getSessionPool(IRetryHandler retryHandler, int maxConcurrentTransactions)
        {
            return new SessionPool(mockCreator.Object, retryHandler, maxConcurrentTransactions, NullLogger.Instance);
        }
    }
}
