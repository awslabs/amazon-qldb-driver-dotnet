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
    using System.Linq;
    using System.Net;
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
    using static TestingUtilities;

    [TestClass]
    public class AsyncQldbDriverTests
    {
        private const string TestTransactionId = "testTransactionId12345";
        private const string TestRequestId = "testId";
        private const string TestLedger = "ledgerName";

        private static AsyncQldbDriverBuilder builder;
        private static MockSessionClient mockClient;
        private static AsyncQldbDriver testDriver;
        private static readonly byte[] digest =
        {
            172, 173, 243, 92, 129, 184, 254, 234, 173, 95, 107, 180, 60, 73, 11, 238,
            6, 87, 197, 229, 178, 142, 155, 122, 218, 197, 23, 15, 241, 117, 92, 132
        };

        [TestInitialize]
        public void SetupTest()
        {
            builder = AsyncQldbDriver.Builder()
                .WithLedger("testLedger")
                .WithRetryLogging()
                .WithLogger(NullLogger.Instance)
                .WithAWSCredentials(new Mock<AWSCredentials>().Object)
                .WithQLDBSessionConfig(new AmazonQLDBSessionConfig());
            Assert.IsNotNull(builder);

            mockClient = new MockSessionClient();
            mockClient.SetDefaultResponse(DefaultSendCommandResponse("testToken", TestTransactionId,
                TestRequestId, digest));

            testDriver = new AsyncQldbDriver(TestLedger, mockClient, 4, NullLogger.Instance, null);
            Assert.IsNotNull(testDriver);
        }

        [TestMethod]
        public void TestAsyncWithPoolLimitArgumentBounds()
        {
            AsyncQldbDriver driver;

            // Default pool limit
            driver = builder.Build();
            Assert.IsNotNull(driver);

            // Negative pool limit
            Assert.ThrowsException<ArgumentException>(() => builder.WithMaxConcurrentTransactions(-4));

            driver = builder.WithMaxConcurrentTransactions(0).Build();
            Assert.IsNotNull(driver);
            driver = builder.WithMaxConcurrentTransactions(4).Build();
            Assert.IsNotNull(driver);
        }

        [TestMethod]
        public async Task TestAsyncListTableNamesLists()
        {
            var factory = new ValueFactory();
            var tables = new List<string> { "table1", "table2" };
            var ions = tables.Select(t => CreateValueHolder(factory.NewString(t))).ToList();

            var h1 = QldbHash.ToQldbHash(TestTransactionId);
            h1 = AsyncTransaction.Dot(h1, QldbDriverBase<AsyncQldbSession>.TableNameQuery, new List<IIonValue>());

            mockClient.QueueResponse(StartSessionResponse(TestRequestId));
            mockClient.QueueResponse(StartTransactionResponse(TestTransactionId, TestRequestId));
            mockClient.QueueResponse(ExecuteResponse(TestRequestId, ions));
            mockClient.QueueResponse(CommitResponse(TestTransactionId, TestRequestId, h1.Hash));

            var result = await testDriver.ListTableNames();

            Assert.IsNotNull(result);
            CollectionAssert.AreEqual(tables, result.ToList());

            mockClient.Clear();
        }

        [TestMethod]
        public async Task TestAsyncGetSession_ExpectedSessionReturned()
        {
            AsyncQldbSession returnedSession = await testDriver.GetSession(CancellationToken.None);
            Assert.IsNotNull(returnedSession);
            Assert.AreEqual(TestRequestId, returnedSession.GetSessionId());
        }

        [TestMethod]
        public async Task TestAsyncGetSession_FailedToCreateSession_ThrowTheOriginalException()
        {
            var exception = new AmazonServiceException("test");
            mockClient.QueueResponse(exception);

            try
            {
                await testDriver.GetSession(CancellationToken.None);

                Assert.Fail("driver.GetSession() should have thrown retriable exception");
            }
            catch (RetriableException re)
            {
                Assert.IsNotNull(re.InnerException);
                Assert.AreEqual(exception, re.InnerException);
            }
        }

        [TestMethod]
        public async Task TestAsyncExecuteWithActionLambdaCanInvokeSuccessfully()
        {
            bool executeInvoked = false;
            try
            {
                await testDriver.Execute(async txn =>
                {
                    await txn.Execute("testStatement");
                    executeInvoked = true;
                });
            }
            catch (Exception)
            {
                Assert.Fail("driver.Execute() should not have thrown exception");
            }

            Assert.IsTrue(executeInvoked);
        }

        [TestMethod]
        public async Task TestAsyncExecuteWithActionAndRetryPolicyCanInvokeSuccessfully()
        {
            bool executeInvoked = false;
            try
            {
                await testDriver.Execute(
                    async txn =>
                    {
                        await txn.Execute("testStatement");
                        executeInvoked = true;
                    },
                    Driver.RetryPolicy.Builder().Build());
            }
            catch (Exception)
            {
                Assert.Fail("driver.Execute() should not have thrown exception");
            }

            Assert.IsTrue(executeInvoked);
        }

        [TestMethod]
        public async Task TestAsyncExecuteWithFuncLambdaReturnsFuncOutput()
        {
            bool executeInvoked = false;
            var result = await testDriver.Execute(async txn =>
            {
                await txn.Execute("testStatement");
                executeInvoked = true;
                return await Task.FromResult("testReturnValue");
            });
            Assert.IsTrue(executeInvoked);
            Assert.AreEqual("testReturnValue", result);
        }

        [TestMethod]
        public async Task TestAsyncExecuteWithFuncLambdaAndRetryPolicyReturnsFuncOutput()
        {
            bool executeInvoked = false;
            var result = await testDriver.Execute(async txn =>
            {
                await txn.Execute("testStatement");
                executeInvoked = true;
                return await Task.FromResult("testReturnValue");
            }, Driver.RetryPolicy.Builder().Build());
            Assert.IsTrue(executeInvoked);
            Assert.AreEqual("testReturnValue", result);
        }

        [TestMethod]
        public async Task TestAsyncExecuteWithFuncLambdaAndRetryPolicyThrowsExceptionAfterDispose()
        {
            testDriver.Dispose();
            await Assert.ThrowsExceptionAsync<QldbDriverException>(
                async () => await testDriver.Execute(
                    async txn =>
                    {
                        await txn.Execute("testStatement");
                        return Task.FromResult("testReturnValue");
                    },
                    Driver.RetryPolicy.Builder().Build()));
        }

        [DataTestMethod]
        [DynamicData(nameof(CreateRetriableExecuteTestData), DynamicDataSourceType.Method)]
        public async Task TestAsyncExecute_RetryOnExceptions(
            Driver.RetryPolicy policy,
            IList<Exception> exceptions,
            bool expectThrow)
        {
            string statement = "DELETE FROM table;";
            var h1 = QldbHash.ToQldbHash(TestTransactionId);
            h1 = Transaction.Dot(h1, statement, new List<IIonValue> { });

            mockClient.QueueResponse(StartSessionResponse(TestRequestId));
            mockClient.QueueResponse(StartTransactionResponse(TestTransactionId, TestRequestId));
            foreach (var ex in exceptions)
            {
                mockClient.QueueResponse(ex);

                // OccConflictException reuses the session so no need for another start session.
                if (ex is not OccConflictException)
                {
                    mockClient.QueueResponse(StartSessionResponse(TestRequestId));
                }

                mockClient.QueueResponse(StartTransactionResponse(TestTransactionId, TestRequestId));
            }
            mockClient.QueueResponse(ExecuteResponse(TestRequestId, null));
            mockClient.QueueResponse(CommitResponse(TestTransactionId, TestRequestId, h1.Hash));

            try
            {
                await testDriver.Execute(txn => txn.Execute(statement), policy);

                Assert.IsFalse(expectThrow);
            }
            catch (Exception e)
            {
                Assert.IsTrue(expectThrow);

                Assert.IsTrue(exceptions.Count > 0);

                // The exception should be the same type as the last exception in our exception list.
                Exception finalException = exceptions[exceptions.Count - 1];
                Type expectedExceptionType = finalException.GetType();
                Assert.IsInstanceOfType(e, expectedExceptionType);
            }

            mockClient.Clear();
        }

        public static IEnumerable<object[]> CreateRetriableExecuteTestData()
        {
            var defaultPolicy = Driver.RetryPolicy.Builder().Build();
            var customerPolicy = Driver.RetryPolicy.Builder().WithMaxRetries(10).Build();

            var capacityExceeded = new CapacityExceededException("qldb", ErrorType.Receiver, "errorCode", "requestId", HttpStatusCode.ServiceUnavailable);
            var occConflict = new OccConflictException("qldb", new BadRequestException("oops"));
            var invalidSession = new InvalidSessionException("invalid session");
            var http500 = new AmazonQLDBSessionException("", 0, "", "", HttpStatusCode.ServiceUnavailable);

            return new List<object[]>() {
                // No exception, No retry.
                new object[] { defaultPolicy, new Exception[0], false },
                // Generic Driver exception.
                new object[] { defaultPolicy, new Exception[] { new QldbDriverException("generic") }, true },
                // Not supported Txn exception.
                new object[] { defaultPolicy, new Exception[] { new QldbTransactionException("txnid1111111",
                    new QldbDriverException("qldb")) }, true },
                // Not supported exception.
                new object[] { defaultPolicy, new Exception[] { new ArgumentException("qldb") }, true },
                // Transaction expiry.
                new object[] { defaultPolicy,
                    new Exception[] { new InvalidSessionException("Transaction 324weqr2314 has expired") }, true },
                // Retry OCC within retry limit.
                new object[] { defaultPolicy, new Exception[] { occConflict, occConflict, occConflict }, false },
                // Retry ISE within retry limit.
                new object[] { defaultPolicy, new Exception[] { invalidSession, invalidSession, invalidSession }, false },
                // Retry mixed exceptions within retry limit.
                new object[] { defaultPolicy, new Exception[] { invalidSession, occConflict, http500 }, false },
                // Retry OCC exceed limit.
                new object[] { defaultPolicy, new Exception[] { occConflict, invalidSession, http500, invalidSession,
                    occConflict }, true },
                // Retry CapacityExceededException exceed limit.
                new object[] { defaultPolicy, new Exception[] { capacityExceeded, capacityExceeded, capacityExceeded,
                    capacityExceeded, capacityExceeded }, true },
                // Retry customized policy within retry limit.
                new object[] { customerPolicy, new Exception[] { invalidSession, invalidSession, invalidSession,
                    invalidSession, invalidSession, invalidSession, invalidSession, invalidSession}, false },
            };
        }
    }
}
