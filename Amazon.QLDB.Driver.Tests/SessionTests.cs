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
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using IonDotnet.Tree;
    using IonDotnet.Tree.Impl;
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
        public static void Setup(TestContext context)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            var testStartSessionResponse = new SendCommandResponse
            {
                StartSession = new StartSessionResult
                {
                    SessionToken = "testSessionToken"
                }
            };
            SetResponse(testStartSessionResponse);

            session = Session.StartSession("testLedgerName", mockClient.Object, NullLogger.Instance);
        }

        [TestMethod]
        public void TestStartSession()
        {
            Assert.AreEqual(mockClient.Object, session.SessionClient);
            Assert.AreEqual("testLedgerName", session.LedgerName);
        }

        [TestMethod]
        public void TestAbortTransaction()
        {
            var testAbortTransactionResult = new AbortTransactionResult();
            var testAbortTransactionResponse = new SendCommandResponse
            {
                AbortTransaction = testAbortTransactionResult
            };
            SetResponse(testAbortTransactionResponse);

            Assert.AreEqual(testAbortTransactionResult, session.AbortTransaction());
        }

        [TestMethod]
        public void TestEndSession()
        {
            var testEndSessionResult = new EndSessionResult();
            var testEndSessionResponse = new SendCommandResponse
            {
                EndSession = testEndSessionResult
            };
            SetResponse(testEndSessionResponse);

            Assert.AreEqual(testEndSessionResult, session.EndSession());
        }

        [TestMethod]
        public void TestEnd()
        {
            mockClient.Setup(x => x.SendCommandAsync(It.IsAny<SendCommandRequest>(), It.IsAny<CancellationToken>()))
                .Throws(new AmazonServiceException());

            session.End();
        }

        [TestMethod]
        public void TestCommitTransaction()
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

            Assert.AreEqual(testCommitTransactionResult, session.CommitTransaction("txnId", new MemoryStream()));
        }

        [TestMethod]
        public void TestExecuteStatement()
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

            var executeResultParams = session.ExecuteStatement("txnId", "statement", parameters);
            Assert.AreEqual(testExecuteStatementResult, executeResultParams);

            var executeResultEmptyParams = session.ExecuteStatement("txnId", "statement", new List<IIonValue>());
            Assert.AreEqual(testExecuteStatementResult, executeResultEmptyParams);
        }

        [TestMethod]
        public void TestFetchPage()
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

            Assert.AreEqual(testFetchPageResult, session.FetchPage("txnId", "nextPageToken"));
        }

        [TestMethod]
        public void TestStartTransaction()
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

            Assert.AreEqual(testStartTransactionResult, session.StartTransaction());
        }

        private static void SetResponse(SendCommandResponse response)
        {
            mockClient.Setup(x => x.SendCommandAsync(It.IsAny<SendCommandRequest>(), It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(response));
        }
    }
}
