
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
        public void RetriableExecute_NoRetry_SuccessfulReturn()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance);

            var func = new Mock<Func<int>>();
            var newSession = new Mock<Action>();
            var nextSession = new Mock<Action>();
            var retry = new Mock<Action<int>>();

            func.Setup(f => f.Invoke()).Returns(1);

            Assert.AreEqual(1, handler.RetriableExecute<int>(func.Object, Driver.RetryPolicy.Builder().Build(),
                newSession.Object, nextSession.Object, retry.Object));

            func.Verify(f => f.Invoke(), Times.Once);
            newSession.Verify(r => r.Invoke(), Times.Never);
            nextSession.Verify(r => r.Invoke(), Times.Never);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Never);
        }

        [TestMethod]
        public void RetriableExecute_NotInListException_ThrowIt()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance);

            var func = new Mock<Func<int>>();
            var newSession = new Mock<Action>();
            var nextSession = new Mock<Action>();
            var retry = new Mock<Action<int>>();

            var exception = new QldbDriverException("qldb");
            func.Setup(f => f.Invoke()).Throws(exception);

            Assert.AreEqual(exception,
                Assert.ThrowsException<QldbDriverException>(() => handler.RetriableExecute<int>(func.Object, Driver.RetryPolicy.Builder().Build(),
                newSession.Object, nextSession.Object, retry.Object)));

            func.Verify(f => f.Invoke(), Times.Once);
            newSession.Verify(r => r.Invoke(), Times.Never);
            nextSession.Verify(r => r.Invoke(), Times.Never);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Never);
        }

        [TestMethod]
        public void RetriableExecute_TransactionExpiryCase_ThrowISE()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance);

            var func = new Mock<Func<int>>();
            var newSession = new Mock<Action>();
            var nextSession = new Mock<Action>();
            var retry = new Mock<Action<int>>();

            var exception = new InvalidSessionException("Transaction 324weqr2314 has expired");
            func.Setup(f => f.Invoke()).Throws(exception);

            Assert.AreEqual(exception,
                Assert.ThrowsException<InvalidSessionException>(() => 
                    handler.RetriableExecute<int>(func.Object, Driver.RetryPolicy.Builder().Build(),
                        newSession.Object, nextSession.Object, retry.Object)));

            func.Verify(f => f.Invoke(), Times.Once);
            newSession.Verify(r => r.Invoke(), Times.Never);
            nextSession.Verify(r => r.Invoke(), Times.Never);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Never);
        }

        [TestMethod]
        public void RetriableExecute_RetryWithoutRecoverWithinLimit_Succeed()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance);

            var func = new Mock<Func<int>>();
            var newSession = new Mock<Action>();
            var nextSession = new Mock<Action>();
            var retry = new Mock<Action<int>>();

            var occ = new RetriableException("txnId11111", true, new OccConflictException("qldb"));
            var retriable = new RetriableException("testTransactionIdddddd", true, new Exception());
            func.SetupSequence(f => f.Invoke()).Throws(occ).Throws(retriable).Throws(occ).Throws(retriable).Returns(1);

            Assert.AreEqual(1, handler.RetriableExecute<int>(func.Object, Driver.RetryPolicy.Builder().Build(),
                newSession.Object, nextSession.Object, retry.Object));

            func.Verify(f => f.Invoke(), Times.Exactly(5));
            newSession.Verify(r => r.Invoke(), Times.Never);
            nextSession.Verify(r => r.Invoke(), Times.Never);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Exactly(4));
        }

        [TestMethod]
        public void RetriableExecute_RetryWithRecoverWithinItsLimit_Succeed()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance);

            var func = new Mock<Func<int>>();
            var newSession = new Mock<Action>();
            var nextSession = new Mock<Action>();
            var retry = new Mock<Action<int>>();

            var invalid = new RetriableException("txnid1", false, new InvalidSessionException("invalid session"));

            func.SetupSequence(f => f.Invoke())
                .Throws(invalid)
                .Throws(invalid)
                .Throws(invalid)
                .Returns(1);

            Assert.AreEqual(1, handler.RetriableExecute<int>(func.Object,
                Driver.RetryPolicy.Builder().WithMaxRetries(4).Build(),
                newSession.Object, nextSession.Object, retry.Object));

            func.Verify(f => f.Invoke(), Times.Exactly(4));
            newSession.Verify(r => r.Invoke(), Times.Exactly(3));
            nextSession.Verify(r => r.Invoke(), Times.Never);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Exactly(3));
        }

        [TestMethod]
        public void RetriableExecute_RetryWithRecoverExceedItsLimit_Succeed()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance);

            var func = new Mock<Func<int>>();
            var newSession = new Mock<Action>();
            var nextSession = new Mock<Action>();
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
                    Driver.RetryPolicy.Builder().WithMaxRetries(4).Build(),
                    newSession.Object, nextSession.Object, retry.Object)));
        }

        [TestMethod]
        public void RetriableExecute_BothLimitedAndRecoverRetryExceptions_SucceedSinceBothAreJustWithinLimit()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance);

            var func = new Mock<Func<int>>();
            var newSession = new Mock<Action>();
            var nextSession = new Mock<Action>();
            var retry = new Mock<Action<int>>();

            var occ = new RetriableException("ddddddd1", true, new OccConflictException("qldb"));
            var invalid = new RetriableException("dddddd2", false, new InvalidSessionException("invalid session"));
            func.SetupSequence(f => f.Invoke())
                .Throws(occ)
                .Throws(invalid)
                .Throws(occ)
                .Returns(1);

            Assert.AreEqual(1, handler.RetriableExecute<int>(func.Object,
                Driver.RetryPolicy.Builder().WithMaxRetries(4).Build(),
                newSession.Object, nextSession.Object, retry.Object));

            func.Verify(f => f.Invoke(), Times.Exactly(4));
            newSession.Verify(r => r.Invoke(), Times.Once);
            nextSession.Verify(r => r.Invoke(), Times.Never);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Exactly(3));
        }

        [TestMethod]
        public void RetriableExecute_RetryMoreThanLimit_ThrowTheLastException()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance);

            var func = new Mock<Func<int>>();
            var newSession = new Mock<Action>();
            var nextSession = new Mock<Action>();
            var retry = new Mock<Action<int>>();

            var occ = new RetriableException("txnid1", true, new OccConflictException("qldb"));
            var ase = new AmazonServiceException();
            var retriable = new RetriableException("testTransactionIdddddd", false, ase);
            func.SetupSequence(f => f.Invoke())
                .Throws(retriable)
                .Throws(occ)
                .Throws(retriable)
                .Throws(occ)
                .Throws(retriable);

            Assert.AreEqual(ase,
                Assert.ThrowsException<AmazonServiceException>(
                    () => handler.RetriableExecute(func.Object, Driver.RetryPolicy.Builder().Build(),
                        newSession.Object, nextSession.Object, retry.Object)));

            func.Verify(f => f.Invoke(), Times.Exactly(5));
            newSession.Verify(r => r.Invoke(), Times.Never);
            nextSession.Verify(r => r.Invoke(), Times.Exactly(2));
        }

        [TestMethod]
        public void RetriableExecute_CustomizedRetryPolicy_ThrowTheLastException()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance);

            var func = new Mock<Func<int>>();
            var newSession = new Mock<Action>();
            var nextSession = new Mock<Action>();
            var retry = new Mock<Action<int>>();

            var occ = new RetriableException("testTransactionIeeee", true, new OccConflictException("qldb"));
            var ase = new AmazonServiceException();
            var retriable = new RetriableException("testTransactionIdddddd", true, ase);
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
                    () => handler.RetriableExecute(func.Object, retryPolicy,
                        newSession.Object, nextSession.Object, retry.Object)));

            func.Verify(f => f.Invoke(), Times.Exactly(3));
            newSession.Verify(r => r.Invoke(), Times.Never);
            nextSession.Verify(r => r.Invoke(), Times.Never);
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
