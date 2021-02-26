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
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.QLDBSession.Model;
    using Microsoft.Extensions.Logging.Abstractions;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class AsyncSessionPoolTests
    {
        private static readonly Mock<IAsyncRetryHandler> mockAsyncRetryHandler = new Mock<IAsyncRetryHandler>();
        private static readonly IAsyncRetryHandler retryHandler = AsyncQldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance);
        private static Mock<Func<AsyncTransactionExecutor, Task<int>>> mockFunction;
        private static Mock<Func<CancellationToken, Task<Session>>> mockSessionCreator;
        private static Mock<Session> mockSession;
        private static StartTransactionResult sendCommandResponseStart;
        private static CommitTransactionResult sendCommandResponseCommit; 

        [TestInitialize]
        public void Setup()
        {
            mockSessionCreator = new Mock<Func<CancellationToken, Task<Session>>>();
            mockSession = new Mock<Session>(null, null, null, null, null);
            mockSessionCreator.Setup(f => f.Invoke(default)).ReturnsAsync(mockSession.Object);

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

            mockSession.Setup(x => x.StartTransactionAsync(It.IsAny<CancellationToken>())).ReturnsAsync(sendCommandResponseStart);
            mockSession.Setup(x => x.CommitTransactionAsync(It.IsAny<string>(), It.IsAny<MemoryStream>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(sendCommandResponseCommit);

            mockFunction = new Mock<Func<AsyncTransactionExecutor, Task<int>>>();
        }

        [TestMethod]
        public void AsyncSessionPoolConstructorTest()
        {
            Assert.IsNotNull(new AsyncSessionPool(mockSessionCreator.Object, mockAsyncRetryHandler.Object, 1, NullLogger.Instance));
        }
        
        [TestMethod]
        public async Task AsyncGetSessionReturnsNewSessionFromPool()
        {
            var asyncPool = getAsyncSessionPool(mockAsyncRetryHandler.Object, 1);
            var returnedSession = await asyncPool.GetSession();

            Assert.IsNotNull(returnedSession);
            mockSessionCreator.Verify(x => x.Invoke(default), Times.Exactly(1));
        }
        
        [TestMethod]
        public async Task AsyncGetSessionReturnsExpectedSessionFromPool()
        {
            var session = new Session(null, null, null, "testSessionId", null);
            mockSessionCreator.Setup(f => f.Invoke(default)).ReturnsAsync(session);

            var asyncPool = getAsyncSessionPool(mockAsyncRetryHandler.Object, 1);
            var returnedSession = await asyncPool.GetSession();

            Assert.AreEqual(session.SessionId, returnedSession.GetSessionId());
        }
        
        [TestMethod]
        public async Task AsyncGetSessionThrowsExceptionWhenGetTwoSessionsFromPoolOfOne()
        {
            var asyncPool = getAsyncSessionPool(mockAsyncRetryHandler.Object, 1);
            var returnedSession = await asyncPool.GetSession();
            
            // Second GetSession
            await Assert.ThrowsExceptionAsync<QldbDriverException>(async () => await asyncPool.GetSession());

            Assert.IsNotNull(returnedSession);
            mockSessionCreator.Verify(x => x.Invoke(default), Times.Exactly(1));
        }
        
        [TestMethod]
        public async Task AsyncGetSessionReturnsCorrectSessionWhenGetTwoSessionsFromPoolOfOneAfterFirstOneDisposed()
        {
            var asyncPool = getAsyncSessionPool(mockAsyncRetryHandler.Object, 1);
            var returnedSession = await asyncPool.GetSession();
            returnedSession.Release();
            
            // Second GetSession
            var nextSession = await  asyncPool.GetSession();
            Assert.IsNotNull(nextSession);

            await nextSession.StartTransaction();
            mockSessionCreator.Verify(x => x.Invoke(default), Times.Exactly(1));
        }

        [TestMethod]
        [ExpectedException(typeof(Runtime.AmazonServiceException), AllowDerivedTypes = false)]
        public async Task AsyncGetSessionThrowsAmazonServiceExceptionWhenFailToCreateSession()
        {
            var exception = new Runtime.AmazonServiceException("test");
            mockSessionCreator.Setup(f => f.Invoke(It.IsAny<CancellationToken>())).ThrowsAsync(exception);

            var asyncPool = getAsyncSessionPool(mockAsyncRetryHandler.Object, 1);

            await asyncPool.GetSession();
        }
        
        [TestMethod]
        public async Task AsyncReleaseSessionShouldNotEndSession()
        {
            var asyncPool = getAsyncSessionPool(mockAsyncRetryHandler.Object, 1);

            var returnedSession = await asyncPool.GetSession();

            returnedSession.Release();

            mockSession.Verify(s => s.End(), Times.Never);
        }
        
        [TestMethod]
        public async Task AsyncExecuteNoRetryNoException()
        {
            mockFunction.Setup(f => f.Invoke(It.IsAny<AsyncTransactionExecutor>())).ReturnsAsync(1);

            var asyncPool = getAsyncSessionPool(retryHandler, 1);

            await asyncPool.Execute(mockFunction.Object, It.IsAny<RetryPolicy>(), It.IsAny<CancellationToken>());

            mockSessionCreator.Verify(x => x.Invoke(default), Times.Once);
            mockFunction.Verify(x => x.Invoke(It.IsAny<AsyncTransactionExecutor>()), Times.Once);
        }
        
        [TestMethod]
        public async Task AsyncExecuteWithOCCExceptionsWithinRetryLimit()
        {
            mockFunction.SetupSequence(f => f.Invoke(It.IsAny<AsyncTransactionExecutor>()))
                .ThrowsAsync(new OccConflictException("occ"))
                .ThrowsAsync(new OccConflictException("occ"))
                .ThrowsAsync(new OccConflictException("occ"))
                .ReturnsAsync(1);

            var asyncPool = getAsyncSessionPool(retryHandler, 1);

            await asyncPool.Execute(mockFunction.Object, RetryPolicy.Builder().Build(), It.IsAny<CancellationToken>());

            mockSessionCreator.Verify(x => x.Invoke(default), Times.Once);
            mockFunction.Verify(x => x.Invoke(It.IsAny<AsyncTransactionExecutor>()), Times.Exactly(4));
            Assert.AreEqual(1, asyncPool.AvailablePermit());
        }
        
        [TestMethod]
        public async Task AsyncExecuteHasOCCExceptionsAndAbortFailuresWithinRetryLimit()
        {
            var abortResponse = new AbortTransactionResult { };
            var serviceException = new Runtime.AmazonServiceException();

            mockSession.SetupSequence(x => x.AbortTransactionAsync(It.IsAny<CancellationToken>()))
                .ThrowsAsync(serviceException)
                .ReturnsAsync(abortResponse)
                .ThrowsAsync(serviceException);

            mockFunction.SetupSequence(f => f.Invoke(It.IsAny<AsyncTransactionExecutor>()))
                .ThrowsAsync(new OccConflictException("occ"))
                .ThrowsAsync(new OccConflictException("occ"))
                .ThrowsAsync(new OccConflictException("occ"))
                .ReturnsAsync(1);

            var asyncPool = getAsyncSessionPool(retryHandler, 2);

            var session1 = await asyncPool.GetSession();
            var session2 = await asyncPool.GetSession();
            session1.Release();
            session2.Release();

            await asyncPool.Execute(mockFunction.Object, RetryPolicy.Builder().Build());

            mockSessionCreator.Verify(x => x.Invoke(default), Times.Exactly(2));
            mockFunction.Verify(x => x.Invoke(It.IsAny<AsyncTransactionExecutor>()), Times.Exactly(4));
            Assert.AreEqual(2, asyncPool.AvailablePermit());
        }
        
        [TestMethod]
        [ExpectedException(typeof(OccConflictException), AllowDerivedTypes = false)]
        public async Task AsyncExecuteWithOCCExceptionsAboveRetryLimitShouldThrowException()
        {
            mockFunction.SetupSequence(f => f.Invoke(It.IsAny<AsyncTransactionExecutor>()))
                .Throws(new OccConflictException("occ"))
                .Throws(new OccConflictException("occ"))
                .Throws(new OccConflictException("occ"))
                .Throws(new OccConflictException("occ"))
                .Throws(new OccConflictException("occ"));

            var asyncPool = getAsyncSessionPool(retryHandler, 1);

            await asyncPool.Execute(mockFunction.Object, RetryPolicy.Builder().Build());

            mockSessionCreator.Verify(x => x.Invoke(default), Times.Once);
        }
        
        [TestMethod]
        public async Task AsyncExecuteWithISEAndTwoSessions()
        {
            mockFunction.SetupSequence(f => f.Invoke(It.IsAny<AsyncTransactionExecutor>()))
                .Throws(new InvalidSessionException("invalid"))
                .Throws(new InvalidSessionException("invalid"))
                .Throws(new InvalidSessionException("invalid"))
                .Throws(new InvalidSessionException("invalid"))
                .ReturnsAsync(1);

            var asyncPool = getAsyncSessionPool(retryHandler, 2);

            var session1 = await asyncPool.GetSession();
            var session2 = await asyncPool.GetSession();
            session1.Release();
            session2.Release();

            await asyncPool.Execute(mockFunction.Object, RetryPolicy.Builder().Build());

            mockSessionCreator.Verify(x => x.Invoke(default), Times.Exactly(6));
            mockFunction.Verify(x => x.Invoke(It.IsAny<AsyncTransactionExecutor>()), Times.Exactly(5));
            Assert.AreEqual(2, asyncPool.AvailablePermit());
        }
        
        [TestMethod]
        public async Task DisposeAsyncSessionPoolShouldDestroyAllSessions()
        {
            var mockSession1 = new Mock<Session>(null, null, null, null, null);
            var mockSession2 = new Mock<Session>(null, null, null, null, null);
            mockSessionCreator.SetupSequence(x => x.Invoke(default)).ReturnsAsync(mockSession1.Object).ReturnsAsync(mockSession2.Object);

            var asyncPool = getAsyncSessionPool(retryHandler, 2);

            var session1 = await asyncPool.GetSession();
            var session2 = await asyncPool.GetSession();
            session1.Release();
            session2.Release();

            asyncPool.Dispose();

            mockSession1.Verify(s => s.End(), Times.Exactly(1));
            mockSession2.Verify(s => s.End(), Times.Exactly(1));
        }

        private static AsyncSessionPool getAsyncSessionPool(IAsyncRetryHandler retryHandler, int maxConcurrentTransactions)
        {
            return new AsyncSessionPool(mockSessionCreator.Object, retryHandler, maxConcurrentTransactions, NullLogger.Instance);
        }
    }
}
