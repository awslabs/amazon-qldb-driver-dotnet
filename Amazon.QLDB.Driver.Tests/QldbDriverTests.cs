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
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging.Abstractions;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;
    using static Amazon.QLDB.Driver.QldbDriver;


    [TestClass]
    public class QldbDriverTests
    {
        private static QldbDriverBuilder builder;
        private static readonly Mock<AmazonQLDBSessionClient> mockClient = new Mock<AmazonQLDBSessionClient>();
        private static readonly byte[] digest = new byte[] { 89, 49, 253, 196, 209, 176, 42, 98, 35, 214, 6, 163, 93,
            141, 170, 92, 75, 218, 111, 151, 173, 49, 57, 144, 227, 72, 215, 194, 186, 93, 85, 108,
        };

        [ClassInitialize]
#pragma warning disable IDE0060 // Remove unused parameter
        public static void Setup(TestContext context)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            var sendCommandResponse = new SendCommandResponse
            {
                StartSession = new StartSessionResult
                {
                    SessionToken = "testToken"
                },
                StartTransaction = new StartTransactionResult
                {
                    TransactionId = "testTransactionIdddddd"
                },
                ExecuteStatement = new ExecuteStatementResult
                {
                    FirstPage = new Page
                    {
                        NextPageToken = null,
                        Values = new List<ValueHolder>()
                    }
                },
                CommitTransaction = new CommitTransactionResult
                {
                    CommitDigest = new MemoryStream(digest)
                },
                ResponseMetadata = new ResponseMetadata
                {
                    RequestId = "testId"
                }
            };
            mockClient.Setup(x => x.SendCommandAsync(It.IsAny<SendCommandRequest>(), It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(sendCommandResponse));
        }

        [TestInitialize]
        public void SetupTest()
        {
            builder = QldbDriver.Builder().WithLedger("testLedger");
        }

        [TestMethod]
        public void TestBuilderReturnsValidBuilder()
        {
            Assert.IsNotNull(builder);
        }

        [TestMethod]
        public void TestWithLedgerValidAndInvalidInputs()
        {
            var testBuilder = QldbDriver.Builder();
            QldbDriver driver;

            // No specified ledger
            Assert.ThrowsException<ArgumentException>(() => driver = testBuilder.Build());

            // Empty ledger
            Assert.ThrowsException<ArgumentException>(() => testBuilder.WithLedger(""));
            Assert.ThrowsException<ArgumentException>(() => testBuilder.WithLedger(null));

            driver = testBuilder.WithLedger("testLedger").Build();
            Assert.IsNotNull(driver);
        }

        [TestMethod]
        public void TestWithAWSCredentialsValidAndInvalidInputs()
        {
            QldbDriver driver;
            AWSCredentials credentials = new BasicAWSCredentials("accessKey", "secretKey");

            // Null credentials
            driver = builder.WithAWSCredentials(null).Build();
            Assert.IsNotNull(driver);

            driver = builder.WithAWSCredentials(credentials).Build();
            Assert.IsNotNull(driver);
        }

        [TestMethod]
        public void TestWithLoggerValidAndInvalidInputs()
        {
            QldbDriver driver;

            // Default logger
            driver = builder.Build();
            Assert.IsNotNull(driver);

            // Null logger
            Assert.ThrowsException<ArgumentException>(() => builder.WithLogger(null));

            driver = builder.WithLogger(NullLogger.Instance).Build();
            Assert.IsNotNull(driver);
        }

        [TestMethod]
        public void TestWithQLDBSessionConfigValidAndInvalidInputs()
        {
            QldbDriver driver;
            var config = new AmazonQLDBSessionConfig();

            // Null config
            driver = builder.WithQLDBSessionConfig(null).Build();
            Assert.IsNotNull(driver);

            driver = builder.WithQLDBSessionConfig(config).Build();
            Assert.IsNotNull(driver);
        }

        [TestMethod]
        public void TestWithRetryLimitParameterBounds()
        {
            QldbDriver driver;

            // Default retry limit
            driver = builder.Build();
            Assert.IsNotNull(driver);

            // Negative retry limit
            Assert.ThrowsException<ArgumentException>(() => builder.WithRetryLimit(-4));

            driver = builder.WithRetryLimit(0).Build();
            Assert.IsNotNull(driver);
            driver = builder.WithRetryLimit(4).Build();
            Assert.IsNotNull(driver);
        }

        [TestMethod]
        public void TestQldbDriverConstructorReturnsValidObject()
        {
            var driver = new QldbDriver("ledgerName", mockClient.Object, 4, NullLogger.Instance);
            Assert.IsNotNull(driver);
        }

        [TestMethod]
        public void TestDisposeWithRepeatCalls()
        {
            var driver = new QldbDriver("ledgerName", mockClient.Object, 4, NullLogger.Instance);
            driver.Dispose();
            driver.Dispose();
        }

        [TestMethod]
        public void TestGetSessionGetsANewSession()
        {
            var driver = new QldbDriver("ledgerName", mockClient.Object, 4, NullLogger.Instance);

            var session1 = driver.GetSession();
            Assert.IsNotNull(session1);

            var session2 = driver.GetSession();
            Assert.IsNotNull(session2);

            Assert.AreNotEqual(session1, session2);

            driver.Dispose();
            Assert.ThrowsException<ObjectDisposedException>(() => driver.GetSession());
        }
    }
}
