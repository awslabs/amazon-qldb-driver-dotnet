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

[assembly: System.Runtime.CompilerServices.InternalsVisibleTo("DynamicProxyGenAssembly2," +
    "PublicKey=0024000004800000940000000602000000240000525341310004000001000100c547cac37abd99" +
    "c8db225ef2f6c8a3602f3b3606cc9891605d02baa56104f4cfc0734aa39b93bf7852f7d9266654753cc297e7" +
    "d2edfe0bac1cdcf9f717241550e0a7b191195b7667bb4f64bcb8e2121380fd1d9d46ad2d92d2d15605093924" +
    "cceaf74c4861eff62abf69b9291ed0a340e113be11e6a7d3113e92484cf7045cc7")] 

namespace Amazon.QLDB.Driver.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using Amazon.IonDotnet.Tree;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging.Abstractions;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class QldbSessionTests
    {
        private static QldbSession qldbSession;
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
            qldbSession = new QldbSession(mockSession.Object, mockAction.Object.DisposeDelegate, NullLogger.Instance);
        }

        [TestMethod]
        public void TestConstructorReturnsValidSession()
        {
            Assert.IsNotNull(qldbSession);
        }

        [TestMethod]
        public void TestReleaseSession()
        {
            qldbSession.Release();
            mockAction.Verify(x => x.DisposeDelegate(qldbSession), Times.Once);
        }

        [TestMethod]
        public void TestQldbSessionConstrcutorReturnsValidObject()
        {
            Assert.IsNotNull(qldbSession);
        }

        [DataTestMethod]
        [DynamicData(nameof(CreateExecuteTestData), DynamicDataSourceType.Method)]
        public void Execute_CustomerTransactionTest(Func<TransactionExecutor, Object> transaction, Object expected, Type expectedExceptionType, Type innerExceptionType,
            Times startTxnTimes, Times executeTimes, Times commitTimes, Times abortTimes, Times retryTimes)
        {
            mockSession.Setup(x => x.StartTransaction()).Returns(new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            });

            mockSession.Setup(x => x.ExecuteStatement(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<List<IIonValue>>())).Returns(new ExecuteStatementResult
                {
                    FirstPage = new Page
                    {
                        NextPageToken = null,
                        Values = new List<ValueHolder>()
                    }
                });

            mockSession.Setup(x => x.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>()))
                .Returns(new CommitTransactionResult
                {
                    CommitDigest = new MemoryStream(digest)
                });

            try
            {
                var result = qldbSession.Execute(transaction);

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

            mockSession.Verify(s => s.StartTransaction(), startTxnTimes);
            mockSession.Verify(s => s.ExecuteStatement(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<List<IIonValue>>()), executeTimes);
            mockSession.Verify(s => s.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>()), commitTimes);

            mockSession.Verify(s => s.AbortTransaction(), abortTimes);
            mockAction.Verify(s => s.DisposeDelegate(qldbSession), retryTimes);
        }

        public static IEnumerable<object[]> CreateExecuteTestData()
        {
            Func<TransactionExecutor, object> executeNormal = txn =>
            {
                txn.Execute("testStatement");
                return "result";
            };

            Func<TransactionExecutor, object> executeAbort = txn =>
            {
                txn.Execute("testStatement");
                txn.Abort();
                return "result";
            };

            Func<TransactionExecutor, object> customerException = txn =>
            {
                txn.Execute("testStatement");
                throw new ArgumentException("some thing wrong");
            };

            return new List<object[]>() {
                new object[] { executeNormal, "result", null, null, Times.Once(), Times.Once(), Times.Once(), Times.Never(), Times.Never() },
                new object[] { executeAbort, null, typeof(TransactionAbortedException), null, Times.Once(), Times.Once(), Times.Never(), Times.Once(), Times.Never() },
                new object[] { customerException, null, typeof(QldbTransactionException), typeof(ArgumentException), Times.Once(), Times.Once(), Times.Never(), Times.Once(), Times.Never() }
            };
        }

        [DataTestMethod]
        [DynamicData(nameof(CreateExceptionTestData), DynamicDataSourceType.Method)]
        public void Execute_ThrowException_ThrowExpectedException(Exception exception,
            Type expectedExceptionType, Type innerExceptionType, Times abortTransactionCalledTimes)
        {
            mockSession.Setup(x => x.StartTransaction()).Returns(new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            });
            mockSession.Setup(x => x.ExecuteStatement(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<List<IIonValue>>()))
                .Throws(exception);

            try
            {
                qldbSession.Execute(
                    (TransactionExecutor txn) => { txn.Execute("testStatement"); return true; });
            }
            catch (Exception e)
            {
                Assert.AreEqual(expectedExceptionType, e.GetType());
                if (innerExceptionType != null)
                {
                    Assert.AreEqual(innerExceptionType, e.InnerException.GetType());
                }
                mockSession.Verify(s => s.End(), Times.Never);
                mockSession.Verify(s => s.AbortTransaction(), abortTransactionCalledTimes);
                return;
            }
            Assert.Fail();
        }

        public static IEnumerable<object[]> CreateExceptionTestData()
        {
            return new List<object[]>() {
                new object[] { new CapacityExceededException("Capacity Exceeded Exception", ErrorType.Receiver, "errorCode", "requestId", HttpStatusCode.ServiceUnavailable),
                    typeof(RetriableException), typeof(CapacityExceededException),
                    Times.Once()},
                new object[] { new AmazonQLDBSessionException("", 0, "", "", HttpStatusCode.InternalServerError),
                    typeof(RetriableException), null,
                    Times.Once()},
                new object[] { new AmazonQLDBSessionException("", 0, "", "", HttpStatusCode.ServiceUnavailable),
                    typeof(RetriableException), null,
                    Times.Once()},
                new object[] { new AmazonQLDBSessionException("", 0, "", "", HttpStatusCode.Unauthorized),
                    typeof(QldbTransactionException), null,
                    Times.Once()},
                new object[] { new OccConflictException("occ"),
                    typeof(RetriableException), typeof(OccConflictException),
                    Times.Never()},
                new object[] { new AmazonServiceException(),
                    typeof(QldbTransactionException), typeof(AmazonServiceException),
                    Times.Once()},
                new object[] { new InvalidSessionException("invalid session"),
                    typeof(RetriableException), typeof(InvalidSessionException),
                    Times.Never()},
                new object[] { new QldbTransactionException(string.Empty, true, new BadRequestException("Bad request")),
                    typeof(QldbTransactionException), typeof(BadRequestException),
                    Times.Never()},
                new object[] { new TransactionAbortedException("testTransactionIdddddd", true),
                    typeof(TransactionAbortedException), null,
                    Times.Never()},
                new object[] { new Exception("Customer Exception"),
                    typeof(QldbTransactionException), typeof(Exception),
                    Times.Once()}
            };
        }

        [TestMethod]
        public void Execute_ThrowBadRequestExceptionOnStartTransaction_ThrowTransactionOpenedException()
        {
            mockSession.Setup(x => x.StartTransaction()).Throws(new BadRequestException("bad request"));

            var ex = Assert.ThrowsException<QldbTransactionException>(
                () => qldbSession.Execute(
                    (TransactionExecutor txn) => { txn.Execute("testStatement"); return true; }));

            Assert.IsTrue(ex.IsSessionAlive);
            mockSession.Verify(s => s.AbortTransaction(), Times.Once);
        }

        [TestMethod]
        public void Execute_ThrowAmazonServiceExceptionOnAbort_ShouldNotThrowAmazonServiceException()
        {
            mockSession.Setup(x => x.StartTransaction()).Throws(new BadRequestException("bad request"));
            mockSession.Setup(x => x.AbortTransaction()).Throws(new AmazonServiceException());

            var ex = Assert.ThrowsException<QldbTransactionException>(
                () => qldbSession.Execute(
                    (TransactionExecutor txn) => { txn.Execute("testStatement"); return true; }));

            Assert.IsFalse(ex.IsSessionAlive);
            mockSession.Verify(s => s.AbortTransaction(), Times.Once);
        }

        [TestMethod]
        [DynamicData(nameof(CreateExceptions), DynamicDataSourceType.Method)]
        public void Execute_StartTransactionThrowExceptions(Exception exception)
        {
            mockSession.Setup(x => x.StartTransaction()).Throws(exception);

            if (exception.GetType() == typeof(Exception) ||
                (exception.GetType() == typeof(AmazonServiceException) && ((AmazonServiceException)exception).StatusCode != HttpStatusCode.InternalServerError && ((AmazonServiceException)exception).StatusCode != HttpStatusCode.ServiceUnavailable))
            {
                Assert.ThrowsException<QldbTransactionException>(
                    () => qldbSession.Execute(
                        (TransactionExecutor txn) => { txn.Execute("testStatement"); return true; }));

            } else {
                Assert.ThrowsException<RetriableException>(
                    () => qldbSession.Execute(
                        (TransactionExecutor txn) => { txn.Execute("testStatement"); return true; }));
            }
        }

        [TestMethod]
        public void TestStartTransactionReturnsANewTransaction()
        {
            mockSession.Setup(x => x.StartTransaction()).Returns(new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            });

            var transaction = qldbSession.StartTransaction();
            Assert.IsNotNull(transaction);
        }

        internal class MockDisposeDelegate
        {
            public virtual void DisposeDelegate(QldbSession session)
            {
            }
        }

        public static IEnumerable<Object[]> CreateExceptions()
        {
            return new List<object[]>()
            {
                new object[] { new InvalidSessionException("message") },
                new object[] { new OccConflictException("message") },
                new object[] { new AmazonServiceException("message", new Exception(), HttpStatusCode.InternalServerError) },
                new object[] { new AmazonServiceException("message", new Exception(), HttpStatusCode.ServiceUnavailable) },
                new object[] { new AmazonServiceException("message", new Exception(), HttpStatusCode.Conflict) },
                new object[] { new Exception("message")},
                new object[] { new CapacityExceededException("message", ErrorType.Receiver, "errorCode", "requestId", HttpStatusCode.ServiceUnavailable) },
            };
        }
    }
}
