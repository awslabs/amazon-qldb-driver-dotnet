
namespace Amazon.QLDB.Driver.Tests
{
    using System;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging.Abstractions;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class RetryHandlerTests
    {
        [TestMethod]
        public void DoesNeedRecover_CheckIfNeedRecover_ShouldReplyCorrectValue()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance, 10);

            Assert.IsTrue(handler.IsRetriable(new OccConflictException("occ")));
            Assert.IsTrue(handler.IsRetriable(new RetriableException("testTransactionIdddddd", new Exception())));
            Assert.IsTrue(handler.IsRetriable(new InvalidSessionException("invalid")));

            Assert.IsFalse(handler.IsRetriable(new AmazonQLDBSessionException("aqse")));
            Assert.IsFalse(handler.IsRetriable(new QldbDriverException("qldb")));

            Assert.IsFalse(handler.NeedsRecover(new OccConflictException("occ")));
            Assert.IsFalse(handler.NeedsRecover(new RetriableException("testTransactionIdddddd", new Exception())));
            Assert.IsTrue(handler.NeedsRecover(new InvalidSessionException("invalid")));
            Assert.IsFalse(handler.NeedsRecover(new TransactionAlreadyOpenException(string.Empty, new Exception())));
        }

        [TestMethod]
        public void RetriableExecute_NoRetry_SuccessfulReturn()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance, 10);

            var func = new Mock<Func<int>>();
            var recover = new Mock<Action>();
            var retry = new Mock<Action<int>>();

            func.Setup(f => f.Invoke()).Returns(1);

            Assert.AreEqual(1, handler.RetriableExecute<int>(func.Object, Driver.RetryPolicy.Builder().Build(), recover.Object, retry.Object));

            func.Verify(f => f.Invoke(), Times.Once);
            recover.Verify(r => r.Invoke(), Times.Never);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Never);
        }

        [TestMethod]
        public void RetriableExecute_NotInListException_ThrowIt()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance, 10);

            var func = new Mock<Func<int>>();
            var recover = new Mock<Action>();
            var retry = new Mock<Action<int>>();

            var exception = new QldbDriverException("qldb");
            func.Setup(f => f.Invoke()).Throws(exception);

            Assert.AreEqual(exception,
                Assert.ThrowsException<QldbDriverException>(() => handler.RetriableExecute<int>(func.Object, Driver.RetryPolicy.Builder().Build(), recover.Object, retry.Object)));

            func.Verify(f => f.Invoke(), Times.Once);
            recover.Verify(r => r.Invoke(), Times.Never);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Never);
        }

        [TestMethod]
        public void RetriableExecute_TransactionExpiryCase_ThrowISE()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance, 10);

            var func = new Mock<Func<int>>();
            var recover = new Mock<Action>();
            var retry = new Mock<Action<int>>();

            var exception = new InvalidSessionException("Transaction 324weqr2314 has expired");
            func.Setup(f => f.Invoke()).Throws(exception);

            Assert.AreEqual(exception,
                Assert.ThrowsException<InvalidSessionException>(() => 
                    handler.RetriableExecute<int>(func.Object, Driver.RetryPolicy.Builder().Build(), recover.Object, retry.Object)));

            func.Verify(f => f.Invoke(), Times.Once);
            recover.Verify(r => r.Invoke(), Times.Never);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Never);
        }

        [TestMethod]
        public void RetriableExecute_RetryWithoutRecoverWithinLimit_Succeed()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance, 10);

            var func = new Mock<Func<int>>();
            var recover = new Mock<Action>();
            var retry = new Mock<Action<int>>();

            var occ = new OccConflictException("qldb");
            var retriable = new RetriableException("testTransactionIdddddd", new Exception());
            func.SetupSequence(f => f.Invoke()).Throws(occ).Throws(retriable).Throws(occ).Throws(retriable).Returns(1);

            Assert.AreEqual(1, handler.RetriableExecute<int>(func.Object, Driver.RetryPolicy.Builder().Build(), recover.Object, retry.Object));

            func.Verify(f => f.Invoke(), Times.Exactly(5));
            recover.Verify(r => r.Invoke(), Times.Never);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Exactly(4));
        }

        [TestMethod]
        public void RetriableExecute_RetryWithRecoverWithinItsLimit_Succeed()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance, 7);

            var func = new Mock<Func<int>>();
            var recover = new Mock<Action>();
            var retry = new Mock<Action<int>>();

            var invalid = new InvalidSessionException("invalid session");

            func.SetupSequence(f => f.Invoke())
                .Throws(invalid)
                .Throws(invalid)
                .Throws(invalid)
                .Throws(invalid)
                .Throws(invalid)
                .Throws(invalid)
                .Returns(1);

            Assert.AreEqual(1, handler.RetriableExecute<int>(func.Object,
                Driver.RetryPolicy.Builder().WithMaxRetries(4).Build(),
                recover.Object,
                retry.Object));

            func.Verify(f => f.Invoke(), Times.Exactly(7));
            recover.Verify(r => r.Invoke(), Times.Exactly(6));
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Exactly(6));
        }

        [TestMethod]
        public void RetriableExecute_RetryWithRecoverExceedItsLimit_Succeed()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance, 7);

            var func = new Mock<Func<int>>();
            var recover = new Mock<Action>();
            var retry = new Mock<Action<int>>();

            var invalid = new InvalidSessionException("invalid session");

            func.SetupSequence(f => f.Invoke())
                .Throws(invalid)
                .Throws(invalid)
                .Throws(invalid)
                .Throws(invalid)
                .Throws(invalid)
                .Throws(invalid)
                .Throws(invalid)
                .Throws(invalid);

            Assert.AreEqual(invalid,
                Assert.ThrowsException<InvalidSessionException>(
                    () => handler.RetriableExecute(func.Object,
                    Driver.RetryPolicy.Builder().WithMaxRetries(4).Build(), recover.Object, retry.Object)));
        }

        [TestMethod]
        public void RetriableExecute_BothLimitedAndRecoverRetryExceptions_SucceedSinceBothAreJustWithinLimit()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance, 3);

            var func = new Mock<Func<int>>();
            var recover = new Mock<Action>();
            var retry = new Mock<Action<int>>();

            var occ = new OccConflictException("qldb");
            var invalid = new InvalidSessionException("invalid session");
            func.SetupSequence(f => f.Invoke())
                .Throws(occ)
                .Throws(invalid)
                .Throws(occ)
                .Throws(invalid)
                .Throws(occ)
                .Throws(invalid)
                .Throws(occ)
                .Returns(1);

            Assert.AreEqual(1, handler.RetriableExecute<int>(func.Object,
                Driver.RetryPolicy.Builder().WithMaxRetries(4).Build(),
                recover.Object,
                retry.Object));

            func.Verify(f => f.Invoke(), Times.Exactly(8));
            recover.Verify(r => r.Invoke(), Times.Exactly(3));
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Exactly(7));
        }

        [TestMethod]
        public void RetriableExecute_RetryMoreThanLimit_ThrowTheLastException()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance, 10);

            var func = new Mock<Func<int>>();
            var recover = new Mock<Action>();
            var retry = new Mock<Action<int>>();

            var occ = new OccConflictException("qldb");
            var ase = new AmazonServiceException();
            var retriable = new RetriableException("testTransactionIdddddd", ase);
            func.SetupSequence(f => f.Invoke())
                .Throws(retriable)
                .Throws(occ)
                .Throws(retriable)
                .Throws(occ)
                .Throws(retriable);

            Assert.AreEqual(ase,
                Assert.ThrowsException<AmazonServiceException>(
                    () => handler.RetriableExecute(func.Object, Driver.RetryPolicy.Builder().Build(), recover.Object, retry.Object)));

            func.Verify(f => f.Invoke(), Times.Exactly(5));
            recover.Verify(r => r.Invoke(), Times.Never);
        }

        [TestMethod]
        public void RetriableExecute_CustomizedRetryPolicy_ThrowTheLastException()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance, 10);

            var func = new Mock<Func<int>>();
            var recover = new Mock<Action>();
            var retry = new Mock<Action<int>>();

            var occ = new OccConflictException("qldb");
            var ase = new AmazonServiceException();
            var retriable = new RetriableException("testTransactionIdddddd", ase);
            func.SetupSequence(f => f.Invoke())
                .Throws(retriable)
                .Throws(occ)
                .Throws(retriable);

            var backoff = new Mock<IBackoffStrategy>();
            backoff.Setup(b => b.CalculateDelay(It.IsAny<RetryPolicyContext>())).Returns(new TimeSpan());

            var retryPolicy = Driver.RetryPolicy.Builder()
                .WithMaxRetries(2)
                .WithBackoffStrategy(backoff.Object)
                .Build();

            Assert.AreEqual(ase,
                Assert.ThrowsException<AmazonServiceException>(
                    () => handler.RetriableExecute(func.Object, retryPolicy, recover.Object, retry.Object)));

            func.Verify(f => f.Invoke(), Times.Exactly(3));
            recover.Verify(r => r.Invoke(), Times.Never);
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
