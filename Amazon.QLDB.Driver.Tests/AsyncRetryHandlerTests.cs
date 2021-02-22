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
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging.Abstractions;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class AsyncRetryHandlerTests
    {
        [DataTestMethod]
        [DynamicData(nameof(CreateRetriableExecuteAsyncTestData), DynamicDataSourceType.Method)]
        public async Task RetriableExecuteAsync_RetryOnExceptions(Driver.RetryPolicy policy, IList<Exception> exceptions, Type expectedExceptionType, Type innerExceptionType,
            Times funcCalledTimes, Times newSessionCalledTimes, Times nextSessionCalledTimes)
        {
            var handler = (AsyncRetryHandler)AsyncQldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance);

            var func = new Mock<Func<CancellationToken, Task<int>>>();
            var newSession = new Mock<Func<CancellationToken, Task>>();
            var nextSession = new Mock<Func<CancellationToken, Task>>();

            var seq = func.SetupSequence(f => f.Invoke(default));
            foreach (var ex in exceptions)
            {
                seq = seq.ThrowsAsync(ex);
            }
            seq.ReturnsAsync(1);

            try
            {
                await handler.RetriableExecute(
                    func.Object,
                    policy,
                    newSession.Object,
                    nextSession.Object);

                Assert.IsNull(expectedExceptionType);
            }
            catch (Exception e)
            {
                Assert.IsNotNull(expectedExceptionType);
                Assert.IsInstanceOfType(e, expectedExceptionType);

                if (innerExceptionType != null)
                {
                    Assert.IsInstanceOfType(e.InnerException, innerExceptionType);
                }
            }

            func.Verify(f => f.Invoke(default), funcCalledTimes);
            newSession.Verify(r => r.Invoke(default), newSessionCalledTimes);
            nextSession.Verify(r => r.Invoke(default), nextSessionCalledTimes);
        }

        public static IEnumerable<object[]> CreateRetriableExecuteAsyncTestData()
        {
            var defaultPolicy = Driver.RetryPolicy.Builder().Build();
            var customerPolicy = Driver.RetryPolicy.Builder().WithMaxRetries(10).Build();

            var cee = new RetriableException("txnId11111", true, new CapacityExceededException("qldb", ErrorType.Receiver, "errorCode", "requestId", HttpStatusCode.ServiceUnavailable));
            var occ = new RetriableException("txnId11111", true, new OccConflictException("qldb", new BadRequestException("oops")));
            var occFailedAbort = new RetriableException("txnId11111", false, new OccConflictException("qldb", new BadRequestException("oops")));
            var txnExpiry = new RetriableException("txnid1111111", false, new InvalidSessionException("Transaction 324weqr2314 has expired"));
            var ise = new RetriableException("txnid1111111", false, new InvalidSessionException("invalid session"));
            var http500 = new RetriableException("txnid1111111", true, new AmazonQLDBSessionException("", 0, "", "", HttpStatusCode.ServiceUnavailable));

            return new List<object[]>() {
                // No exception, No retry.
                new object[] { defaultPolicy, new Exception[0], null, null,
                    Times.Once(), Times.Never(), Times.Never() },
                // Not supported Txn exception.
                new object[] { defaultPolicy, new Exception[] { new QldbTransactionException("txnid1111111", new QldbDriverException("qldb")) }, typeof(QldbDriverException), null,
                    Times.Once(), Times.Never(), Times.Never() },
                // Not supported exception.
                new object[] { defaultPolicy, new Exception[] { new ArgumentException("qldb") }, typeof(ArgumentException), null,
                    Times.Once(), Times.Never(), Times.Never() },
                // Transaction expiry.
                new object[] { defaultPolicy, new Exception[] { txnExpiry }, typeof(InvalidSessionException), null,
                    Times.Once(), Times.Never(), Times.Never() },
                // Retry OCC within retry limit.
                new object[] { defaultPolicy, new Exception[] { occ, occ, occ }, null, null,
                    Times.Exactly(4), Times.Never(), Times.Never() },
                // Retry ISE within retry limit.
                new object[] { defaultPolicy, new Exception[] { ise, ise, ise }, null, null,
                    Times.Exactly(4), Times.Exactly(3), Times.Never() },
                // Retry mixed exceptions within retry limit.
                new object[] { defaultPolicy, new Exception[] { ise, occ, http500 }, null, null,
                    Times.Exactly(4), Times.Exactly(1), Times.Never() },
                // Retry OCC exceed limit.
                new object[] { defaultPolicy, new Exception[] { occ, ise, http500, ise, occ }, typeof(OccConflictException), null,
                    Times.Exactly(5), Times.Exactly(2), Times.Never() },
                // Retry CapacityExceededException exceed limit.
                new object[] { defaultPolicy, new Exception[] { cee, cee, cee, cee, cee }, typeof(CapacityExceededException), null,
                    Times.Exactly(5), Times.Never(), Times.Never() },
                // Retry OCC with abort txn failures.
                new object[] { defaultPolicy, new Exception[] { occFailedAbort, occ, occFailedAbort }, null, null,
                    Times.Exactly(4), Times.Never(), Times.Exactly(2) },
                // Retry customized policy within retry limit.
                new object[] { customerPolicy, new Exception[] { ise, ise, ise, ise, ise, ise, ise, ise}, null, null,
                    Times.Exactly(9), Times.Exactly(8), Times.Never() },
            };
        }

        [TestMethod]
        public void TestAsyncTransactionExpiryMatchShouldMatchTransactionExpireCases()
        {
            Assert.IsTrue(AsyncRetryHandler.IsTransactionExpiry(new InvalidSessionException("Transaction 324weqr2314 has expired")));

            Assert.IsFalse(AsyncRetryHandler.IsTransactionExpiry(new InvalidSessionException("Transaction 324weqr2314 has not expired")));
        }
    }
}
