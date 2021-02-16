
namespace Amazon.QLDB.Driver.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging.Abstractions;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class RetryHandlerTests
    {
        [DataTestMethod]
        [DynamicData(nameof(CreateRetriableExecuteTestData), DynamicDataSourceType.Method)]
        public void RetriableExecute_RetryOnExceptions(Driver.RetryPolicy policy, IList<Exception> exceptions, Type expectedExceptionType, Type innerExceptionType,
            Times funcCalledTimes, Times newSessionCalledTimes, Times nextSessionCalledTimes, Times retryActionCalledTimes)
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance);

            var func = new Mock<Func<int>>();
            var newSession = new Mock<Action>();
            var nextSession = new Mock<Action>();
            var retry = new Mock<Action<int>>();

            var seq = func.SetupSequence(f => f.Invoke());
            foreach (var ex in exceptions)
            {
                seq = seq.Throws(ex);
            }
            seq.Returns(1);

            try
            {
                handler.RetriableExecute<int>(func.Object,
                    policy,
                    newSession.Object, nextSession.Object, retry.Object);

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

            func.Verify(f => f.Invoke(), funcCalledTimes);
            newSession.Verify(r => r.Invoke(), newSessionCalledTimes);
            nextSession.Verify(r => r.Invoke(), nextSessionCalledTimes);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), retryActionCalledTimes);
        }

        public static IEnumerable<object[]> CreateRetriableExecuteTestData()
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
                    Times.Once(), Times.Never(), Times.Never(), Times.Never() },
                // Not supported Txn exception.
                new object[] { defaultPolicy, new Exception[] { new QldbTransactionException("txnid1111111", new QldbDriverException("qldb")) }, typeof(QldbDriverException), null,
                    Times.Once(), Times.Never(), Times.Never(), Times.Never() },
                // Not supported exception.
                new object[] { defaultPolicy, new Exception[] { new ArgumentException("qldb") }, typeof(ArgumentException), null,
                    Times.Once(), Times.Never(), Times.Never(), Times.Never() },
                // Transaction expiry.
                new object[] { defaultPolicy, new Exception[] { txnExpiry }, typeof(InvalidSessionException), null,
                    Times.Once(), Times.Never(), Times.Never(), Times.Never() },
                // Retry OCC within retry limit.
                new object[] { defaultPolicy, new Exception[] { occ, occ, occ }, null, null,
                    Times.Exactly(4), Times.Never(), Times.Never(), Times.Exactly(3) },
                // Retry ISE within retry limit.
                new object[] { defaultPolicy, new Exception[] { ise, ise, ise }, null, null,
                    Times.Exactly(4), Times.Exactly(3), Times.Never(), Times.Exactly(3) },
                // Retry mixed exceptions within retry limit.
                new object[] { defaultPolicy, new Exception[] { ise, occ, http500 }, null, null,
                    Times.Exactly(4), Times.Exactly(1), Times.Never(), Times.Exactly(3) },
                // Retry OCC exceed limit.
                new object[] { defaultPolicy, new Exception[] { occ, ise, http500, ise, occ }, typeof(OccConflictException), null,
                    Times.Exactly(5), Times.Exactly(2), Times.Never(), Times.Exactly(4) },
                // Retry CapacityExceededException exceed limit.
                new object[] { defaultPolicy, new Exception[] { cee, cee, cee, cee, cee }, typeof(CapacityExceededException), null,
                    Times.Exactly(5), Times.Never(), Times.Never(), Times.Exactly(4) },
                // Retry OCC with abort txn failures.
                new object[] { defaultPolicy, new Exception[] { occFailedAbort, occ, occFailedAbort }, null, null,
                    Times.Exactly(4), Times.Never(), Times.Exactly(2), Times.Exactly(3) },
                // Retry customized policy within retry limit.
                new object[] { customerPolicy, new Exception[] { ise, ise, ise, ise, ise, ise, ise, ise}, null, null,
                    Times.Exactly(9), Times.Exactly(8), Times.Never(), Times.Exactly(8) },
            };
        }

        [TestMethod]
        public void RetryPolicyContext_Create_ShouldReturnCorrectProperties()
        {
            var exception = new Exception();
            var retries = 3;

            var context = new RetryPolicyContext(retries, exception);

            Assert.AreEqual(retries, context.RetriesAttempted);
            Assert.AreEqual(exception, context.LastException);
        }

        [TestMethod]
        public void IsTransactionExpiry_Match_ShouldMatchTransactionExpireCases()
        {
            Assert.IsTrue(RetryHandler.IsTransactionExpiry(new InvalidSessionException("Transaction 324weqr2314 has expired")));

            Assert.IsFalse(RetryHandler.IsTransactionExpiry(new InvalidSessionException("Transaction 324weqr2314 has not expired")));
        }
    }
}
