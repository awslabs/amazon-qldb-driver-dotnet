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
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.IonDotnet.Tree;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class AsyncTransactionTests
    {
        private const string TxnId = "txnId";
        private static AsyncTransaction asyncTransaction;
        private static Mock<Session> mockSession;
        private static readonly Mock<ILogger> mockLogger = new Mock<ILogger>();

        [TestInitialize]
        public void SetUp()
        {
            mockSession = new Mock<Session>(null, null, null, null, null);
            asyncTransaction = new AsyncTransaction(mockSession.Object, TxnId, mockLogger.Object,
                It.IsAny<CancellationToken>());
        }

        [TestMethod]
        public async Task TestAbortCallsAbortTransactionAsync()
        {
            await asyncTransaction.Abort();
            mockSession.Verify(s => s.AbortTransactionAsync(It.IsAny<CancellationToken>()), Times.Exactly(1));
        }

        [TestMethod]
        public async Task TestCommitCallsCommitTransaction()
        {
            mockSession.Setup(s => s.CommitTransactionAsync(It.IsAny<string>(), It.IsAny<MemoryStream>(),
                It.IsAny<CancellationToken>())).Returns(GetAsyncTransactionResult(TxnId));

            // We must not have any exceptions
            await asyncTransaction.Commit();
        }

        [TestMethod]
        [ExpectedException(typeof(InvalidSessionException), AllowDerivedTypes = false)]
        public async Task TestCommitWhenInvalidSessionExceptionShouldThrowException()
        {
            mockSession.Setup(s => s.CommitTransactionAsync(It.IsAny<string>(), It.IsAny<MemoryStream>(),
                It.IsAny<CancellationToken>())).Throws(new InvalidSessionException(It.IsAny<string>()));

            await asyncTransaction.Commit();
            mockSession.Verify(s => s.AbortTransactionAsync(It.IsAny<CancellationToken>()), Times.Never);
        }
        
        [TestMethod]
        [ExpectedException(typeof(OccConflictException), AllowDerivedTypes = false)]
        public async Task TestCommitWhenOCCShouldThrowException()
        {
            mockSession.Setup(s => s.CommitTransactionAsync(It.IsAny<string>(), It.IsAny<MemoryStream>(),
                It.IsAny<CancellationToken>())).Throws(new OccConflictException(It.IsAny<string>()));

            await asyncTransaction.Commit();
            mockSession.Verify(s => s.AbortTransactionAsync(It.IsAny<CancellationToken>()), Times.Never);
        }

        [TestMethod]
        [ExpectedException(typeof(AmazonServiceException), AllowDerivedTypes = false)]
        public async Task TestCommitWhenAmazonServiceExceptionShouldThrowException()
        {
            mockSession.Setup(s => s.CommitTransactionAsync(It.IsAny<string>(), It.IsAny<MemoryStream>(),
                It.IsAny<CancellationToken>())).Throws(new AmazonServiceException(It.IsAny<string>()));

            await asyncTransaction.Commit();
            mockSession.Verify(s => s.AbortTransactionAsync(It.IsAny<CancellationToken>()), Times.Exactly(1));
        }
        
        [TestMethod]
        [ExpectedException(typeof(InvalidOperationException), AllowDerivedTypes = false)]
        public async Task TestCommitWhenDifferentTxnIDThrowsException()
        {
            mockSession.Setup(s => s.CommitTransactionAsync(It.IsAny<string>(), It.IsAny<MemoryStream>(),
                It.IsAny<CancellationToken>())).Returns(GetAsyncTransactionResult("differentTnxIdFromThis"));

            await asyncTransaction.Commit();
        }
        
        [TestMethod]
        public async Task TestExecuteCallsExecuteStatement()
        {
            mockSession.Setup(s => s.ExecuteStatementAsync(It.IsAny<string>(), It.IsAny<string>(),
                It.IsAny<List<IIonValue>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new ExecuteStatementResult
                {
                    FirstPage = new Page
                    {
                        NextPageToken = "nextPageToken",
                        Values = new List<ValueHolder>()
                    }
                });


            var result = await asyncTransaction.Execute("statement");

            mockSession.Verify(s => s.ExecuteStatementAsync(TxnId, "statement", It.IsAny<List<IIonValue>>(),
                It.IsAny<CancellationToken>()));
            Assert.IsNotNull(result);
        }
     
        [TestMethod]
        [ExpectedException(typeof(ArgumentException), AllowDerivedTypes = false)]
        public async Task TestExecuteWhenStatementEmptyThrowsException()
        {
            await asyncTransaction.Execute("");
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        internal static async Task<CommitTransactionResult> GetAsyncTransactionResult(string txnId)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
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
