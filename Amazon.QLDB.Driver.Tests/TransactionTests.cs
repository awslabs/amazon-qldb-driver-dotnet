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
    using System.Threading;
    using System.Threading.Tasks;
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
        public async Task TestAbortCallsAbortTransaction()
        {
            await transaction.Abort();
            mockSession.Verify(m => m.AbortTransaction(It.IsAny<CancellationToken>()), Times.Exactly(1));
        }

        [TestMethod]
        public async Task TestDisposeCallsAbortException()
        {
            await transaction.DisposeAsync();
            mockSession.Verify(m => m.AbortTransaction(It.IsAny<CancellationToken>()), Times.Exactly(1));
        }

        [TestMethod]
        public async Task TestDisposeWhenExceptionIsProperlyIgnored()
        {
            mockSession.Setup(m => m.AbortTransaction(It.IsAny<CancellationToken>())).Throws(new AmazonServiceException(It.IsAny<string>()));

            // Dispose should not throw exception
            await transaction.DisposeAsync();
            mockSession.Verify(m => m.AbortTransaction(It.IsAny<CancellationToken>()), Times.Exactly(1));
        }

        [TestMethod]
        public async Task TestCommitCallsCommitTransaction()
        {
            mockSession.Setup(m => m.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(GetTransactionResult(TxnId));

            // We must not have any exceptions
            await transaction.Commit(It.IsAny<CancellationToken>());
        }

        [TestMethod]
        public async Task Commit_InvalidSessionException_ShouldNotDisposeTransaction()
        {
            mockSession.Setup(m => m.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new InvalidSessionException("Invalid!"));

            await Assert.ThrowsExceptionAsync<InvalidSessionException>(() => transaction.Commit());
            mockSession.Verify(s => s.AbortTransaction(It.IsAny<CancellationToken>()), Times.Exactly(0));
        }

        [TestMethod]
        public async Task TestCommitWhenOCCShouldThrowException()
        {
            mockSession.Setup(m => m.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new OccConflictException(It.IsAny<string>()));

            await Assert.ThrowsExceptionAsync<OccConflictException>(() => transaction.Commit());
            mockSession.Verify(s => s.AbortTransaction(It.IsAny<CancellationToken>()), Times.Exactly(0));
        }

        [TestMethod]
        public async Task TestCommitWhenAmazonServiceExceptionShouldThrowException()
        {
            mockSession.Setup(m => m.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>(), It.IsAny<CancellationToken>()))
                .ThrowsAsync(new AmazonServiceException(It.IsAny<string>()));

            await Assert.ThrowsExceptionAsync<AmazonServiceException>(() => transaction.Commit());
            mockSession.Verify(s => s.AbortTransaction(It.IsAny<CancellationToken>()), Times.Exactly(1));
        }

        [TestMethod]
        public async Task TestCommitWhenDifferentTxnIDThrowsException()
        {
            mockSession.Setup(m => m.CommitTransaction(It.IsAny<string>(), It.IsAny<MemoryStream>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(GetTransactionResult("differentTnxIdFromThis"));

            await Assert.ThrowsExceptionAsync<InvalidOperationException>(() => transaction.Commit());
        }

        [TestMethod]
        public async Task TestExecuteCallsExecuteStatement()
        {
            mockSession.Setup(m => m.ExecuteStatement(It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<List<IIonValue>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ExecuteStatementResult
                {
                    FirstPage = new Page
                    {
                        NextPageToken = "nextPageToken",
                        Values = new List<ValueHolder>()
                    }
                });


            var result = await transaction.Execute("statement");

            mockSession.Verify(m => m.ExecuteStatement(TxnId, "statement", It.IsAny<List<IIonValue>>(), It.IsAny<CancellationToken>()));
            Assert.IsNotNull(result);
        }

        [TestMethod]
        public async Task TestExecuteWhenStatementEmptyThrowsException()
        {
            await Assert.ThrowsExceptionAsync<ArgumentException>(() => transaction.Execute(""));
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
