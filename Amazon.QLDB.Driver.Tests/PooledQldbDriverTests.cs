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
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging.Abstractions;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;
    using static Amazon.QLDB.Driver.PooledQldbDriver;


    [TestClass]
    public class PooledQldbDriverTests
    {
        private static PooledQldbDriverBuilder builder;
        private static Mock<AmazonQLDBSessionClient> mockClient;

        [TestInitialize]
        public void SetupTest()
        {
            mockClient = new Mock<AmazonQLDBSessionClient>();
            var sendCommandResponse = new SendCommandResponse
            {
                StartSession = new StartSessionResult
                {
                    SessionToken = "testToken"
                },
                ResponseMetadata = new ResponseMetadata
                {
                    RequestId = "testId"
                }
            };
            mockClient.Setup(x => x.SendCommandAsync(It.IsAny<SendCommandRequest>(), It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(sendCommandResponse));
            builder = PooledQldbDriver.Builder().WithLedger("testLedger");
        }

        [TestMethod]
        public void TestBuilder()
        {
            Assert.IsNotNull(builder);
        }

        [TestMethod]
        public void TestWithPoolLimit()
        {
            PooledQldbDriver driver;

            // Default pool limit
            driver = builder.Build();
            Assert.IsNotNull(driver);

            // Negative pool limit
            Assert.ThrowsException<ArgumentException>(() => builder.WithPoolLimit(-4));

            driver = builder.WithPoolLimit(0).Build();
            Assert.IsNotNull(driver);
            driver = builder.WithPoolLimit(4).Build();
            Assert.IsNotNull(driver);
        }

        [TestMethod]
        public void TestWithTimeout()
        {
            PooledQldbDriver driver;

            // Default timeout
            driver = builder.Build();
            Assert.IsNotNull(driver);

            // Negative timeout
            Assert.ThrowsException<ArgumentException>(() => builder.WithTimeout(-4));

            driver = builder.WithTimeout(0).Build();
            Assert.IsNotNull(driver);
            driver = builder.WithTimeout(4).Build();
            Assert.IsNotNull(driver);
        }

        [TestMethod]
        public void TestPooledQldbDriver()
        {
            var driver = new PooledQldbDriver("ledgerName", mockClient.Object, 4, 4, 4, NullLogger.Instance);
            Assert.IsNotNull(driver);
        }

        [TestMethod]
        public void TestDispose()
        {
            var driver = new PooledQldbDriver("ledgerName", mockClient.Object, 4, 4, 4, NullLogger.Instance);
            var session = driver.GetSession();
            session.Dispose();
            driver.Dispose();
            driver.Dispose();
            // Once to start session, and once to end session in pool upon disposal of driver.
            mockClient.Verify(x => x.SendCommandAsync(It.IsAny<SendCommandRequest>(), It.IsAny<CancellationToken>()),
                Times.Exactly(2));
        }

        [TestMethod]
        public void TestGetSession()
        {
            var driver = new PooledQldbDriver("ledgerName", mockClient.Object, 4, 4, 4, NullLogger.Instance);
            var session = driver.GetSession();
            Assert.IsNotNull(session);
            // Start session.
            mockClient.Verify(x => x.SendCommandAsync(It.IsAny<SendCommandRequest>(), It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [TestMethod]
        public void TestGetSessionReturnedToPool()
        {
            var sendCommandResponseWithStartSession = new SendCommandResponse
            {
                StartSession = new StartSessionResult
                {
                    SessionToken = "testToken"
                },
                ResponseMetadata = new ResponseMetadata
                {
                    RequestId = "testId"
                }
            };
            var sendCommandResponseEmpty = new SendCommandResponse();
            mockClient.SetupSequence(x => x.SendCommandAsync(It.IsAny<SendCommandRequest>(), It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(sendCommandResponseWithStartSession))
                    .Returns(Task.FromResult(sendCommandResponseEmpty));
            builder = PooledQldbDriver.Builder().WithLedger("testLedger");

            var driver = new PooledQldbDriver("ledgerName", mockClient.Object, 4, 4, 4, NullLogger.Instance);
            var session = driver.GetSession();
            session.Dispose();
            session = driver.GetSession();
            Assert.IsNotNull(session);
            // Once for start session, once for abort. Second will throw exception if start session due to empty response.
            mockClient.Verify(x => x.SendCommandAsync(It.IsAny<SendCommandRequest>(), It.IsAny<CancellationToken>()),
                Times.Exactly(2));
        }

        [TestMethod]
        public void TestGetSessionTimeout()
        {
            var driver = new PooledQldbDriver("ledgerName", mockClient.Object, 4, 1, 10, NullLogger.Instance);
            var session = driver.GetSession();
            Assert.IsNotNull(session);
            Assert.ThrowsException<TimeoutException>(driver.GetSession);
            // Start session.
            mockClient.Verify(x => x.SendCommandAsync(It.IsAny<SendCommandRequest>(), It.IsAny<CancellationToken>()),
                Times.Once);
        }

        [TestMethod]
        public void TestGetSessionNewSessionException()
        {
            var testException = new AmazonClientException("");
            mockClient.Setup(x => x.SendCommandAsync(It.IsAny<SendCommandRequest>(), It.IsAny<CancellationToken>()))
                .Throws(testException);
            var driver = new PooledQldbDriver("ledgerName", mockClient.Object, 4, 1, 10, NullLogger.Instance);
            Assert.ThrowsException<AmazonClientException>(driver.GetSession);
            // Ensure semaphore released due to earlier exception. Will throw TimeoutException if not.
            Assert.ThrowsException<AmazonClientException>(driver.GetSession);
            // Start session twice.
            mockClient.Verify(x => x.SendCommandAsync(It.IsAny<SendCommandRequest>(), It.IsAny<CancellationToken>()),
                Times.Exactly(2));
        }

        [TestMethod]
        public void TestDisposeSessionReleasesSemaphore()
        {
            var driver = new PooledQldbDriver("ledgerName", mockClient.Object, 4, 1, 10, NullLogger.Instance);
            var session = driver.GetSession();
            session.Dispose();
            // Pool limit is 1 so will throw TimeoutException if semaphore was not released upon dispose.
            session = driver.GetSession();
        }
    }
}
