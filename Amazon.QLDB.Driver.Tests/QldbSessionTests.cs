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
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using IonDotnet.Tree;
    using Microsoft.Extensions.Logging.Abstractions;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class QldbSessionTests
    {
        private static QldbSession qldbSession;
        private static Mock<Session> mockSession;
        private static readonly byte[] digest = new byte[] { 89, 49, 253, 196, 209, 176, 42, 98, 35, 214, 6, 163, 93,
            141, 170, 92, 75, 218, 111, 151, 173, 49, 57, 144, 227, 72, 215, 194, 186, 93, 85, 108,
        };
        private static readonly byte[] tableNameDigest = new byte[] { 74, 241, 166, 213, 255, 79, 206, 123, 125, 76, 2,
            77, 4, 141, 74, 225, 141, 20, 87, 7, 142, 87, 99, 123, 64, 107, 231, 142, 34, 137, 178, 113
        };

        [TestInitialize]
        public void SetupTest()
        {
            mockSession = new Mock<Session>(null, null, null, null);
            qldbSession = new QldbSession(mockSession.Object, 2, NullLogger.Instance);
        }

        [TestMethod]
        public void TestQldbSession()
        {
            Assert.IsNotNull(qldbSession);
        }

        [TestMethod]
        public void TestDispose()
        {
            // Throw on second End() call
            mockSession.SetupSequence(x => x.End()).Pass().Throws(new InvalidOperationException());

            qldbSession.Dispose();

            // End() should not be called since the QldbSession is already closed
            qldbSession.Dispose();
        }

        [TestMethod]
        public void TestExecuteStatement()
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

            var result = qldbSession.Execute(
                "testStatement",
                null,
                (int retry) => retryCount = retry);
            Assert.AreEqual(1, txnCount);
            Assert.AreEqual(1, executeCount);
            Assert.AreEqual(1, commitCount);
            Assert.AreEqual(0, retryCount);
        }

        [TestMethod]
        public void TestExecuteAction()
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

            qldbSession.Execute(
                (TransactionExecutor txn) => { txn.Execute("testStatement"); },
                (int retry) => retryCount = retry);
            Assert.AreEqual(1, txnCount);
            Assert.AreEqual(1, executeCount);
            Assert.AreEqual(1, commitCount);
            Assert.AreEqual(0, retryCount);
        }

        [TestMethod]
        public void TestExecuteFunc()
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
        public void TestExecuteAbortException()
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

            Assert.ThrowsException<AbortException>(() => qldbSession.Execute(
                (TransactionExecutor txn) =>
                {
                    txn.Execute("testStatement");
                    txn.Abort();
                },
                (int retry) => retryCount = retry));
            Assert.AreEqual(1, txnCount);
            Assert.AreEqual(1, executeCount);
            Assert.AreEqual(0, commitCount);
            Assert.AreEqual(0, retryCount);
        }

        [TestMethod]
        public void TestExecuteInvalidSessionException()
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
                (TransactionExecutor txn) => { txn.Execute("testStatement"); },
                (int retry) => retryCount = retry));
            Assert.AreEqual(3, txnCount);
            Assert.AreEqual(0, commitCount);
            Assert.AreEqual(2, retryCount);
        }

        [TestMethod]
        public void TestExecuteOccConflictException()
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
                (TransactionExecutor txn) => { txn.Execute("testStatement"); },
                (int retry) => retryCount = retry));
            Assert.AreEqual(3, txnCount);
            Assert.AreEqual(3, executeCount);
            Assert.AreEqual(0, commitCount);
            Assert.AreEqual(2, retryCount);
        }

        [TestMethod]
        public void TestExecuteAmazonQLDBSessionException()
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
                (TransactionExecutor txn) => { txn.Execute("testStatement"); },
                (int retry) => retryCount = retry));
            Assert.AreEqual(3, txnCount);
            Assert.AreEqual(0, commitCount);
            Assert.AreEqual(2, retryCount);
        }

        [TestMethod]
        public void TestExecuteAmazonQLDBSessionExceptionNoRetry()
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
                (TransactionExecutor txn) => { txn.Execute("testStatement"); },
                (int retry) => retryCount = retry));
            Assert.AreEqual(1, txnCount);
            Assert.AreEqual(1, executeCount);
            Assert.AreEqual(0, commitCount);
            Assert.AreEqual(0, retryCount);
        }

        [TestMethod]
        public void TestExecuteOccConflictExceptionRetry()
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
                (TransactionExecutor txn) => { txn.Execute("testStatement"); },
                (int retry) => retryCount = retry);
            Assert.AreEqual(2, txnCount);
            Assert.AreEqual(1, commitCount);
            Assert.AreEqual(1, retryCount);
        }

        [TestMethod]
        public void TestListTableNames()
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
                    CommitDigest = new MemoryStream(tableNameDigest)
                }).Callback(() => commitCount++);

            var tableNames = qldbSession.ListTableNames();
            Assert.AreEqual(1, txnCount);
            Assert.AreEqual(1, executeCount);
            Assert.AreEqual(1, commitCount);
            Assert.AreEqual(0, retryCount);
            Assert.IsNotNull(tableNames);
        }

        [TestMethod]
        public void TestStartTransaction()
        {
            mockSession.Setup(x => x.StartTransaction()).Returns(new StartTransactionResult
            {
                TransactionId = "testTransactionIdddddd"
            });

            var transaction = qldbSession.StartTransaction();
            Assert.IsNotNull(transaction);

            qldbSession.Dispose();
            Assert.ThrowsException<InvalidOperationException>(qldbSession.StartTransaction);
        }
    }
}
