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
            qldbSession = new QldbSession(mockSession.Object, 2, mockAction.Object.DisposeDelegate, NullLogger.Instance);
        }

        [TestMethod]
        public void TestConstructorReturnsValidSession()
        {
            Assert.IsNotNull(qldbSession);
        }

        [TestMethod]
        public void TestDisposeCallsDisposeDelegate()
        {
            qldbSession.Dispose();
            qldbSession.Dispose();
            mockAction.Verify(x => x.DisposeDelegate(qldbSession), Times.Exactly(1));
        }

        [TestMethod]
        public void TestStartTransactionGetsNewTransactionOrThrowsWhenDisposed()
        {
            var mockTransaction = new Mock<StartTransactionResult>();
            mockSession.Setup(x => x.StartTransaction()).Returns(mockTransaction.Object);

            Assert.IsNotNull(qldbSession.StartTransaction());

            qldbSession.Dispose();
            Assert.ThrowsException<QldbDriverException>(qldbSession.StartTransaction);
        }

        [TestMethod]
        public void TestQldbSessionConstrcutorReturnsValidObject()
        {
            Assert.IsNotNull(qldbSession);
        }

        [TestMethod]
        public void TestDisposeCanBeInvokedMultipleTimesButNotEndSession()
        {
            // Throw on second End() call
            mockSession.SetupSequence(x => x.End()).Pass().Throws(new ObjectDisposedException(ExceptionMessages.SessionClosed));

            qldbSession.Dispose();

            // End() should not be called since the QldbSession is already closed
            qldbSession.Dispose();
        }

        [TestMethod]
        public void TestExecuteFuncUsesTransactionLifecycle()
        {
            int txnCount = 0;
            int executeCount = 0;
            int commitCount = 0;
            int retryCount = 0;
            mockSession.Setup(x => x.StartTransaction()).Returns(new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            }).Callback(() => txnCount++);
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
                }).Callback(() => executeCount++);
            mockSession.Setup(x => x.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>()))
                .Returns(new CommitTransactionResult
                {
                    CommitDigest = new MemoryStream(digest)
                }).Callback(() => commitCount++);

            static string testFunc(TransactionExecutor txn)
            {
                txn.Execute("testStatement");
                return "test";
            }
            var result = qldbSession.Execute(
                testFunc,
                (int retry) => retryCount = retry);
            Assert.AreEqual(1, txnCount);
            Assert.AreEqual(1, executeCount);
            Assert.AreEqual(1, commitCount);
            Assert.AreEqual(0, retryCount);
            Assert.AreEqual("test", result);
        }

        [TestMethod]
        public void TestExecuteAbortExceptionAbortsTransactionAndRethrowsException()
        {
            int txnCount = 0;
            int executeCount = 0;
            int commitCount = 0;
            int retryCount = 0;
            mockSession.Setup(x => x.StartTransaction()).Returns(new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            }).Callback(() => txnCount++);
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
                }).Callback(() => executeCount++);
            mockSession.Setup(x => x.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>()))
                .Returns(new CommitTransactionResult
                {
                    CommitDigest = new MemoryStream(digest)
                }).Callback(() => commitCount++);

            Assert.ThrowsException<TransactionAbortedException>(() => qldbSession.Execute(
                (TransactionExecutor txn) =>
                {
                    txn.Execute("testStatement");
                    txn.Abort();
                    return true;
                },
                (int retry) => retryCount = retry));
            Assert.AreEqual(1, txnCount);
            Assert.AreEqual(1, executeCount);
            Assert.AreEqual(0, commitCount);
            Assert.AreEqual(0, retryCount);
        }

        [TestMethod]
        public void TestExecuteInvalidSessionExceptionThrowsIfExceedRetryLimit()
        {
            int txnCount = 0;
            int commitCount = 0;
            int retryCount = 0;
            mockSession.Setup(x => x.StartTransaction()).Returns(new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            }).Callback(() => txnCount++);
            mockSession.SetupSequence(x => x.ExecuteStatement(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<List<IIonValue>>()))
                .Throws(new OccConflictException(""))
                .Throws(new OccConflictException(""))
                .Throws(new InvalidSessionException(""));
            mockSession.Setup(x => x.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>()))
                .Returns(new CommitTransactionResult
                {
                    CommitDigest = new MemoryStream(digest)
                }).Callback(() => commitCount++);

            Assert.ThrowsException<InvalidSessionException>(() => qldbSession.Execute(
                (TransactionExecutor txn) => { txn.Execute("testStatement"); return true; },
                (int retry) => retryCount = retry));
            Assert.AreEqual(3, txnCount);
            Assert.AreEqual(0, commitCount);
            Assert.AreEqual(2, retryCount);
        }

        [TestMethod]
        public void TestExecuteOccConflictExceptionThrowsIfExceedRetryLimit()
        {
            int txnCount = 0;
            int executeCount = 0;
            int commitCount = 0;
            int retryCount = 0;
            mockSession.Setup(x => x.StartTransaction()).Returns(new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            }).Callback(() => txnCount++);
            mockSession.Setup(x => x.ExecuteStatement(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<List<IIonValue>>()))
                .Callback(() => executeCount++)
                .Throws(new OccConflictException(""));
            mockSession.Setup(x => x.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>()))
                .Returns(new CommitTransactionResult
                {
                    CommitDigest = new MemoryStream(digest)
                }).Callback(() => commitCount++);

            Assert.ThrowsException<OccConflictException>(() => qldbSession.Execute(
                (TransactionExecutor txn) => { txn.Execute("testStatement"); return true; },
                (int retry) => retryCount = retry));
            Assert.AreEqual(3, txnCount);
            Assert.AreEqual(3, executeCount);
            Assert.AreEqual(0, commitCount);
            Assert.AreEqual(2, retryCount);
        }

        [TestMethod]
        public void TestExecuteAmazonQLDBSessionExceptionThrowsIfRetryLimitExceeded()
        {
            int txnCount = 0;
            int commitCount = 0;
            int retryCount = 0;
            mockSession.Setup(x => x.StartTransaction()).Returns(new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            }).Callback(() => txnCount++);
            mockSession.SetupSequence(x => x.ExecuteStatement(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<List<IIonValue>>()))
                .Throws(new AmazonQLDBSessionException("", 0, "", "", HttpStatusCode.InternalServerError))
                .Throws(new AmazonQLDBSessionException("", 0, "", "", HttpStatusCode.ServiceUnavailable))
                .Throws(new AmazonQLDBSessionException("", 0, "", "", HttpStatusCode.ServiceUnavailable));
            mockSession.Setup(x => x.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>()))
                .Returns(new CommitTransactionResult
                {
                    CommitDigest = new MemoryStream(digest)
                }).Callback(() => commitCount++);

            Assert.ThrowsException<AmazonQLDBSessionException>(() => qldbSession.Execute(
                (TransactionExecutor txn) => { txn.Execute("testStatement"); return true; },
                (int retry) => retryCount = retry));
            Assert.AreEqual(3, txnCount);
            Assert.AreEqual(0, commitCount);
            Assert.AreEqual(2, retryCount);
        }

        [TestMethod]
        public void TestExecuteAmazonQLDBSessionExceptionDoesNotRetryIfNotRetriable()
        {
            int txnCount = 0;
            int executeCount = 0;
            int commitCount = 0;
            int retryCount = 0;
            mockSession.Setup(x => x.StartTransaction()).Returns(new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            }).Callback(() => txnCount++);
            mockSession.Setup(x => x.ExecuteStatement(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<List<IIonValue>>()))
                .Callback(() => executeCount++)
                .Throws(new AmazonQLDBSessionException("", 0, "", "", HttpStatusCode.NotFound));
            mockSession.Setup(x => x.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>()))
                .Returns(new CommitTransactionResult
                {
                    CommitDigest = new MemoryStream(digest)
                }).Callback(() => commitCount++);

            Assert.ThrowsException<AmazonQLDBSessionException>(() => qldbSession.Execute(
                (TransactionExecutor txn) => { txn.Execute("testStatement"); return true; },
                (int retry) => retryCount = retry));
            Assert.AreEqual(1, txnCount);
            Assert.AreEqual(1, executeCount);
            Assert.AreEqual(0, commitCount);
            Assert.AreEqual(0, retryCount);
        }

        [TestMethod]
        public void TestExecuteOccConflictExceptionRetriesExecution()
        {
            int txnCount = 0;
            int commitCount = 0;
            int retryCount = 0;
            mockSession.Setup(x => x.StartTransaction()).Returns(new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            }).Callback(() => txnCount++);
            mockSession.SetupSequence(x => x.ExecuteStatement(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<List<IIonValue>>()))
                .Throws(new OccConflictException(""))
                .Returns(new ExecuteStatementResult
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
                }).Callback(() => commitCount++);

           qldbSession.Execute(
                (TransactionExecutor txn) => { txn.Execute("testStatement"); return true; },
                (int retry) => retryCount = retry);
            Assert.AreEqual(2, txnCount);
            Assert.AreEqual(1, commitCount);
            Assert.AreEqual(1, retryCount);
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

            qldbSession.Dispose();
            Assert.ThrowsException<QldbDriverException>(qldbSession.StartTransaction);
        }


        [TestMethod]
        public void Renew_RenewDisposedSession_ShouldNotThrow()
        {
            mockSession.Setup(x => x.StartTransaction()).Returns(new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            });

            var transaction = qldbSession.StartTransaction();
            Assert.IsNotNull(transaction);

            qldbSession.Dispose();
            Assert.ThrowsException<QldbDriverException>(qldbSession.StartTransaction);

            qldbSession.Renew();
            qldbSession.StartTransaction();
        }

        internal class MockDisposeDelegate
        {
            public virtual void DisposeDelegate(QldbSession session)
            {
            }
        }
    }
}
