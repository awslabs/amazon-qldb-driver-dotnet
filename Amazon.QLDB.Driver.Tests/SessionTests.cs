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
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.IonDotnet.Tree;
    using Amazon.IonDotnet.Tree.Impl;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging.Abstractions;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class SessionTests
    {
        private static readonly Mock<AmazonQLDBSessionClient> mockClient = new Mock<AmazonQLDBSessionClient>();
        private static Session session;

        [ClassInitialize]
#pragma warning disable IDE0060 // Remove unused parameter
        public static async Task Setup(TestContext context)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            var testStartSessionResponse = new SendCommandResponse
            {
                StartSession = new StartSessionResult
                {
                    SessionToken = "testSessionToken"
                },
                ResponseMetadata = new ResponseMetadata
                {
                    RequestId = "testId"
                }
            };
            SetResponse(testStartSessionResponse);

            session = await Session.StartSession("testLedgerName", mockClient.Object, NullLogger.Instance);
        }

        [TestMethod]
        public void TestStartSessionReturnsSessionWithMatchingParameters()
        {
            Assert.AreEqual(mockClient.Object, session.SessionClient);
            Assert.AreEqual("testLedgerName", session.LedgerName);
        }

        [TestMethod]
        public async Task TestAbortTransactionReturnsResponse()
        {
            var testAbortTransactionResult = new AbortTransactionResult();
            var testAbortTransactionResponse = new SendCommandResponse
            {
                AbortTransaction = testAbortTransactionResult
            };
            SetResponse(testAbortTransactionResponse);

            Assert.AreEqual(testAbortTransactionResult, await session.AbortTransaction());
        }

        [TestMethod]
        public async Task TestEndSessionReturnsResponse()
        {
            var testEndSessionResult = new EndSessionResult();
            var testEndSessionResponse = new SendCommandResponse
            {
                EndSession = testEndSessionResult
            };
            SetResponse(testEndSessionResponse);

            Assert.AreEqual(testEndSessionResult, await session.EndSession());
        }

        [TestMethod]
        public async Task TestEndSendsEndSessionAndIgnoresExceptions()
        {
            mockClient.Setup(x => x.SendCommandAsync(It.IsAny<SendCommandRequest>(), It.IsAny<CancellationToken>()))
                .Throws(new AmazonServiceException());

            await session.End();
        }

        [TestMethod]
        public async Task TestCommitTransactionReturnsResponse()
        {
            var testCommitTransactionResult = new CommitTransactionResult
            {
                TransactionId = "testTxnIdddddddddddddd",
                CommitDigest = new MemoryStream()
            };
            var testCommitTransactionResponse = new SendCommandResponse
            {
                CommitTransaction = testCommitTransactionResult
            };
            SetResponse(testCommitTransactionResponse);

            Assert.AreEqual(testCommitTransactionResult, await session.CommitTransaction("txnId", new MemoryStream()));
        }

        [TestMethod]
        public async Task TestExecuteStatementReturnsResponse()
        {
            var testExecuteStatementResult = new ExecuteStatementResult
            {
                FirstPage = new Page()
            };
            var testExecuteStatementResponse = new SendCommandResponse
            {
                ExecuteStatement = testExecuteStatementResult
            };
            SetResponse(testExecuteStatementResponse);

            IValueFactory valueFactory = new ValueFactory();
            var parameters = new List<IIonValue>
            {
                valueFactory.NewString("param1"),
                valueFactory.NewString("param2")
            };

            var executeResultParams = await session.ExecuteStatement("txnId", "statement", parameters);
            Assert.AreEqual(testExecuteStatementResult, executeResultParams);

            var executeResultEmptyParams = await session.ExecuteStatement("txnId", "statement", new List<IIonValue>());
            Assert.AreEqual(testExecuteStatementResult, executeResultEmptyParams);
        }

        [TestMethod]
        public async Task TestFetchPageReturnsResponse()
        {
            var testFetchPageResult = new FetchPageResult
            {
                Page = new Page()
            };
            var testFetchPageResponse = new SendCommandResponse
            {
                FetchPage = testFetchPageResult
            };
            SetResponse(testFetchPageResponse);

            Assert.AreEqual(testFetchPageResult, await session.FetchPage("txnId", "nextPageToken"));
        }

        [TestMethod]
        public async Task TestStartTransactionReturnsResponse()
        {
            var testStartTransactionResult = new StartTransactionResult
            {
                TransactionId = "testTxnIdddddddddddddd"
            };
            var testStartTransactionResponse = new SendCommandResponse
            {
                StartTransaction = testStartTransactionResult
            };
            SetResponse(testStartTransactionResponse);

            Assert.AreEqual(testStartTransactionResult, await session.StartTransaction());
        }

        private static void SetResponse(SendCommandResponse response)
        {
            mockClient.Setup(x => x.SendCommandAsync(It.IsAny<SendCommandRequest>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(response);
        }
    }
}
