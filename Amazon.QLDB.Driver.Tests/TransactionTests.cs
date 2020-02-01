﻿/*
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
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using IonDotnet.Tree;
    using Microsoft.Extensions.Logging;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class TransactionTests
    {
        private const string tnxId = "My-transactionID-value";
        private static Transaction transaction;
        private static Mock<Session> mockSession;
        private static Mock<ILogger> mockLogger = new Mock<ILogger>();

        [TestInitialize]
        public void SetUp()
        {
            mockSession = new Mock<Session>(null, null, null, null);
            transaction = new Transaction(mockSession.Object, tnxId, mockLogger.Object);
        }

        [TestMethod]
        public void TestAbort()
        {
            transaction.Abort();
            mockSession.Verify(m => m.AbortTransaction(), Times.Exactly(1));
        }

        [TestMethod]
        public void TestDispose()
        {
            transaction.Dispose();
            mockSession.Verify(m => m.AbortTransaction(), Times.Exactly(1));
        }

        [TestMethod]
        public void TestDisposeWhenException()
        {
            mockSession.Setup(m => m.AbortTransaction()).Throws(new AmazonClientException(It.IsAny<string>()));

            // Dispose should not throw exception
            transaction.Dispose();
            mockSession.Verify(m => m.AbortTransaction(), Times.Exactly(1));
        }

        [TestMethod]
        public void TestCommit()
        {
            mockSession.Setup(m => m.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>()))
                .Returns(GetTransactionResult(tnxId));

            // We must not have any exceptions
            transaction.Commit();
        }

        [TestMethod]
        public void TestCommitWhenClosed()
        {
            // To have our transaction marked closed
            transaction.Abort();

            Assert.ThrowsException<InvalidOperationException>(transaction.Commit);
        }

        [TestMethod]
        public void TestCommitWhenOCC()
        {
            mockSession.Setup(m => m.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>()))
                .Throws(new OccConflictException(It.IsAny<string>()));

            Assert.ThrowsException<OccConflictException>(transaction.Commit);
        }

        [TestMethod]
        public void TestCommitWhenAmazonClientException()
        {
            mockSession.Setup(m => m.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>()))
                .Throws(new AmazonClientException(It.IsAny<string>()));

            Assert.ThrowsException<AmazonClientException>(transaction.Commit);
        }

        [TestMethod]
        public void TestCommitWhenDifferentTxnID()
        {
            mockSession.Setup(m => m.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>()))
                .Returns(GetTransactionResult("differentTnxIdFromThis"));

            Assert.ThrowsException<InvalidOperationException>(transaction.Commit);
        }

        [TestMethod]
        public void TestExecute()
        {
            mockSession.Setup(m => m.ExecuteStatement(It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<List<IIonValue>>()))
                .Returns(new ExecuteStatementResult
                {
                    FirstPage = new Page
                    {
                        NextPageToken = "nextPageToken",
                        Values = new List<ValueHolder>()
                    }
                });


            var result = transaction.Execute("statement");

            mockSession.Verify(m => m.ExecuteStatement(tnxId, "statement", It.IsAny<List<IIonValue>>()));
            Assert.IsNotNull(result);
        }

        [TestMethod]
        public void TestExecuteWhenStatementEmpty()
        {
            Assert.ThrowsException<ArgumentException>(() => transaction.Execute(""));
        }

        [TestMethod]
        public void TestExecuteWhenClosed()
        {
            // To have our transaction marked closed
            transaction.Abort();

            Assert.ThrowsException<InvalidOperationException>(() => transaction.Execute("statement"));
        }


        private CommitTransactionResult GetTransactionResult(string txnId)
        {
            var hashBytes = QldbHash.ToQldbHash(txnId).Hash;
            CommitTransactionResult commitTransactionResult = new CommitTransactionResult
            {
                TransactionId = txnId,
                CommitDigest = new MemoryStream(hashBytes)
            };

            return commitTransactionResult;
        }
    }
}
