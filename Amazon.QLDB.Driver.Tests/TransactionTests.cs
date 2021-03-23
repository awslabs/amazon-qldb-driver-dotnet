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
    using Amazon.IonDotnet.Tree;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class TransactionTests
    {
        private const string TxnId = "My-transactionID-value";
        private static Transaction transaction;
        private static Mock<Session> mockSession;
        private static readonly Mock<ILogger> mockLogger = new Mock<ILogger>();

        [TestInitialize]
        public void SetUp()
        {
            mockSession = new Mock<Session>(null, null, null, null, null);
            transaction = new Transaction(mockSession.Object, TxnId, mockLogger.Object);
        }

        [TestMethod]
        public void TestAbortCallsAbortTransaction()
        {
            transaction.Abort();
            mockSession.Verify(m => m.AbortTransaction(), Times.Exactly(1));
        }

        [TestMethod]
        public void TestCommitCallsCommitTransaction()
        {
            mockSession.Setup(m => m.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>()))
                .Returns(GetTransactionResult(TxnId));

            // We must not have any exceptions
            transaction.Commit();
        }

        [TestMethod]
        public void Commit_InvalidSessionException_ShouldNotDisposeTransaction()
        {
            mockSession.Setup(m => m.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>()))
                .Throws(new InvalidSessionException("Invalid!"));

            Assert.ThrowsException<InvalidSessionException>(transaction.Commit);
            mockSession.Verify(s => s.AbortTransaction(), Times.Exactly(0));
        }

        [TestMethod]
        public void TestCommitWhenOCCShouldThrowException()
        {
            mockSession.Setup(m => m.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>()))
                .Throws(new OccConflictException(It.IsAny<string>()));

            Assert.ThrowsException<OccConflictException>(transaction.Commit);
            mockSession.Verify(s => s.AbortTransaction(), Times.Exactly(0));
        }

        [TestMethod]
        public void TestCommitWhenAmazonServiceExceptionShouldThrowException()
        {
            mockSession.Setup(m => m.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>()))
                .Throws(new AmazonServiceException(It.IsAny<string>()));

            Assert.ThrowsException<AmazonServiceException>(transaction.Commit);
            mockSession.Verify(s => s.AbortTransaction(), Times.Exactly(0));
        }

        [TestMethod]
        public void TestCommitWhenDifferentTxnIDThrowsException()
        {
            mockSession.Setup(m => m.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>()))
                .Returns(GetTransactionResult("differentTnxIdFromThis"));

            Assert.ThrowsException<InvalidOperationException>(transaction.Commit);
        }

        [TestMethod]
        public void TestExecuteCallsExecuteStatement()
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

            mockSession.Verify(m => m.ExecuteStatement(TxnId, "statement", It.IsAny<List<IIonValue>>()));
            Assert.IsNotNull(result);
        }

        [TestMethod]
        public void TestExecuteWhenStatementEmptyThrowsException()
        {
            Assert.ThrowsException<ArgumentException>(() => transaction.Execute(""));
        }

        private CommitTransactionResult GetTransactionResult(string txnId)
        {
            return AsyncTransactionTests.GetAsyncTransactionResult(txnId).GetAwaiter().GetResult();
        }
    }
}
