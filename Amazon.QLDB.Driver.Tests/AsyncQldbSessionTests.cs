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
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.IonDotnet.Tree;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging.Abstractions;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class AsyncQldbSessionTests
    {
        private static AsyncQldbSession asyncQldbSession;
        private static Mock<Session> mockSession;
        private static Mock<MockDisposeDelegate> mockAction;
        private static readonly byte[] digest = new byte[] { 89, 49, 253, 196, 209, 176, 42, 98, 35, 214, 6, 163, 93,
            141, 170, 92, 75, 218, 111, 151, 173, 49, 57, 144, 227, 72, 215, 194, 186, 93, 85, 108,
        };
        private static readonly byte[] tableNameDigest = new byte[] { 74, 241, 166, 213, 255, 79, 206, 123, 125, 76, 2,
            77, 4, 141, 74, 225, 141, 20, 87, 7, 142, 87, 99, 123, 64, 107, 231, 142, 34, 137, 178, 113
        };

        [TestInitialize]
        public void SetupTest()
        {
            mockAction = new Mock<MockDisposeDelegate>();
            mockSession = new Mock<Session>(null, null, null, null, null);
            asyncQldbSession = new AsyncQldbSession(mockSession.Object, NullLogger.Instance);
        }

        [TestMethod]
        public void TestAsyncQldbSessionConstrcutorReturnsValidObject()
        {
            Assert.IsNotNull(asyncQldbSession);
        }

        [DataTestMethod]
        [DynamicData(nameof(CreateExecuteTestData), DynamicDataSourceType.Method)]
        public async Task ExecuteAsyncTransactionTest(Func<AsyncTransactionExecutor, Task<Object>> transaction, Object expected, Type expectedExceptionType,
            Type innerExceptionType, Times startTxnTimes, Times executeTimes, Times commitTimes, Times abortTimes, Times retryTimes)
        {
            mockSession.Setup(s => s.StartTransactionAsync(It.IsAny<CancellationToken>())).ReturnsAsync(new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            });

            mockSession.Setup(x => x.ExecuteStatementAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<List<IIonValue>>(),
                It.IsAny<CancellationToken>())).ReturnsAsync(new ExecuteStatementResult
                {
                    FirstPage = new Page
                    {
                        NextPageToken = null,
                        Values = new List<ValueHolder>()
                    }
                });

            mockSession.Setup(x => x.CommitTransactionAsync(It.IsAny<string>(), It.IsAny<MemoryStream>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new CommitTransactionResult
                {
                    CommitDigest = new MemoryStream(digest)
                });

            try
            {
                var result = await asyncQldbSession.Execute(transaction);

                Assert.IsNull(expectedExceptionType);
                Assert.AreEqual(expected, result);
            }
            catch (Exception e)
            {
                Assert.IsNotNull(expectedExceptionType);

                Assert.IsInstanceOfType(e, expectedExceptionType);

                if (innerExceptionType != null)
                {
                    Assert.IsInstanceOfType(e.InnerException, innerExceptionType);
                }
            }

            mockSession.Verify(s => s.StartTransactionAsync(It.IsAny<CancellationToken>()), startTxnTimes);
            mockSession.Verify(s => s.ExecuteStatementAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<List<IIonValue>>(), It.IsAny<CancellationToken>()), executeTimes);
            mockSession.Verify(s => s.CommitTransactionAsync(It.IsAny<string>(), It.IsAny<MemoryStream>(), It.IsAny<CancellationToken>()), commitTimes);

            mockSession.Verify(s => s.AbortTransactionAsync(It.IsAny<CancellationToken>()), abortTimes);
            mockAction.Verify(s => s.DisposeDelegate(asyncQldbSession), retryTimes);
        }

        private static IEnumerable<object[]> CreateExecuteTestData()
        {
            Func<AsyncTransactionExecutor, Task<object>> executeNormal = async txn =>
            {
                await txn.Execute("testStatement");
                return "result";
            };

            Func<AsyncTransactionExecutor, Task<object>> executeAbort = async txn =>
            {
                await txn.Execute("testStatement");
                await txn.Abort();
                return "result";
            };

            Func<AsyncTransactionExecutor, Task<object>> customerException = async txn =>
            {
                await txn.Execute("testStatement");
                throw new ArgumentException("some thing wrong");
            };

            return new List<object[]>() {
                new object[] { executeNormal, "result", null, null, Times.Once(), Times.Once(), Times.Once(), Times.Never(), Times.Never() },
                new object[] { executeAbort, null, typeof(TransactionAbortedException), null, Times.Once(), Times.Once(), Times.Never(), Times.Once(), Times.Never() },
                new object[] { customerException, null, typeof(QldbTransactionException), typeof(ArgumentException), Times.Once(), Times.Once(), Times.Never(), Times.Once(), Times.Never() }
            };
        }

        [TestMethod]
        [TestingUtilities.ExecuteExceptionTestHelper]
        public async Task ExecuteAsyncThrowsExpectedException(Exception exception, Type expectedExceptionType, Type innerExceptionType, Times abortTransactionCalledTimes)
        {
            mockSession.Setup(x => x.StartTransactionAsync(It.IsAny<CancellationToken>())).ReturnsAsync(new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            });
            mockSession.Setup(x => x.ExecuteStatementAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<List<IIonValue>>(),
                It.IsAny<CancellationToken>()))
                .ThrowsAsync(exception);

            try
            {
                await asyncQldbSession.Execute(
                    async (AsyncTransactionExecutor txn) => { await txn.Execute("testStatement"); return true; });
            }
            catch (Exception e)
            {
                Assert.AreEqual(expectedExceptionType, e.GetType());
                if (innerExceptionType != null)
                {
                    Assert.AreEqual(innerExceptionType, e.InnerException.GetType());
                }
                mockSession.Verify(s => s.End(), Times.Never);
                mockSession.Verify(s => s.AbortTransactionAsync(It.IsAny<CancellationToken>()), abortTransactionCalledTimes);
                return;
            }
            Assert.Fail();
        }

        [TestMethod]
        [ExpectedException(typeof(QldbTransactionException), AllowDerivedTypes = false)]
        public void ExecutAsyncThrowsQldbTransactionExceptionWhenStartTransactionAsyncThrowBadRequestException()
        {
            mockSession.Setup(x => x.StartTransactionAsync(It.IsAny<CancellationToken>())).Throws(new BadRequestException("bad request"));

            var ex = asyncQldbSession.Execute(
                    async (AsyncTransactionExecutor txn) => { await txn.Execute("testStatement"); return true; });

            Assert.IsTrue(ex.GetAwaiter().GetResult());
            mockSession.Verify(s => s.AbortTransactionAsync(It.IsAny<CancellationToken>()), Times.Once);
        }

        [TestMethod]
        public async Task AsyncExecuteShouldThrowQldbTransactionExceptionWhenAbortTransactionAsyncThrowsAmazonServiceException()
        {
            mockSession.Setup(x => x.StartTransactionAsync(It.IsAny<CancellationToken>())).Throws(new BadRequestException("bad request"));
            mockSession.Setup(x => x.AbortTransactionAsync(It.IsAny<CancellationToken>())).Throws(new AmazonServiceException());

            var ex = await Assert.ThrowsExceptionAsync<QldbTransactionException>(
                async () => await asyncQldbSession.Execute(
                    async (AsyncTransactionExecutor txn) => { await txn.Execute("testStatement"); return true; }));

            Assert.IsFalse(ex.IsSessionAlive);
            mockSession.Verify(s => s.AbortTransactionAsync(It.IsAny<CancellationToken>()), Times.Once);
        }

        [TestMethod]
        [TestingUtilities.CreateExceptions]
        public async Task TestAsyncExecuteWhenStartTransactionAsyncThrowExceptions(Exception exception)
        {
            mockSession.Setup(x => x.StartTransactionAsync(It.IsAny<CancellationToken>())).Throws(exception);

            if (exception.GetType() == typeof(Exception) ||
                (exception.GetType() == typeof(AmazonServiceException) &&
                ((AmazonServiceException)exception).StatusCode != HttpStatusCode.InternalServerError &&
                ((AmazonServiceException)exception).StatusCode != HttpStatusCode.ServiceUnavailable))
            {
                await Assert.ThrowsExceptionAsync<QldbTransactionException>(
                    async () => await asyncQldbSession.Execute(
                        async (AsyncTransactionExecutor txn) => { await txn.Execute("testStatement"); return true; }));

            }
            else
            {
                await Assert.ThrowsExceptionAsync<RetriableException>(
                    async () => await asyncQldbSession.Execute(
                        async (AsyncTransactionExecutor txn) => { await txn.Execute("testStatement"); return true; }));
            }
        }

        [TestMethod]
        public void StartTransactionAsyncReturnsANewTransactionTest()
        {
            mockSession.Setup(x => x.StartTransactionAsync(It.IsAny<CancellationToken>())).ReturnsAsync(new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            });

            var transaction = asyncQldbSession.StartTransaction();
            Assert.IsNotNull(transaction);
        }

        internal class MockDisposeDelegate
        {
            public virtual void DisposeDelegate(AsyncQldbSession session)
            {
            }
        }
    }
}
