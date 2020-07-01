namespace Amazon.QLDB.Driver.Tests
{
    using System;
    using System.Threading.Tasks;
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
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance);

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
        public async Task RetriableExecute_NoRetry_SuccessfulReturn()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance);

            var func = new Mock<Func<Task<int>>>();
            var recover = new Mock<Func<Task>>();
            var retry = new Mock<Func<int, Task>>();

            func.Setup(f => f.Invoke()).ReturnsAsync(1);

            Assert.AreEqual(1, await handler.RetriableExecute<int>(func.Object, Driver.RetryPolicy.Builder().Build(), recover.Object, retry.Object));

            func.Verify(f => f.Invoke(), Times.Once);
            recover.Verify(r => r.Invoke(), Times.Never);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Never);
        }

        [TestMethod]
        public async Task RetriableExecute_NotInListException_ThrowIt()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance);

            var func = new Mock<Func<Task<int>>>();
            var recover = new Mock<Func<Task>>();
            var retry = new Mock<Func<int, Task>>();

            var exception = new QldbDriverException("qldb");
            func.Setup(f => f.Invoke()).Throws(exception);

            Assert.AreEqual(exception,
                await Assert.ThrowsExceptionAsync<QldbDriverException>(() => handler.RetriableExecute<int>(func.Object, Driver.RetryPolicy.Builder().Build(), recover.Object, retry.Object)));

            func.Verify(f => f.Invoke(), Times.Once);
            recover.Verify(r => r.Invoke(), Times.Never);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Never);
        }

        [TestMethod]
        public async Task RetriableExecute_RetryWithoutRecoverWithinLimit_Succeed()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance);

            var func = new Mock<Func<Task<int>>>();
            var recover = new Mock<Func<Task>>();
            var retry = new Mock<Func<int, Task>>();

            var occ = new OccConflictException("qldb");
            var retriable = new RetriableException("testTransactionIdddddd", new Exception());
            func.SetupSequence(f => f.Invoke())
                .ThrowsAsync(occ)
                .ThrowsAsync(retriable)
                .ThrowsAsync(occ)
                .ThrowsAsync(retriable)
                .ReturnsAsync(1);

            Assert.AreEqual(1, await handler.RetriableExecute<int>(func.Object, Driver.RetryPolicy.Builder().Build(), recover.Object, retry.Object));

            func.Verify(f => f.Invoke(), Times.Exactly(5));
            recover.Verify(r => r.Invoke(), Times.Never);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Exactly(4));
        }

        [TestMethod]
        public async Task RetriableExecute_RetryWithRecoverRegardlessLimit_Succeed()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance);

            var func = new Mock<Func<Task<int>>>();
            var recover = new Mock<Func<Task>>();
            var retry = new Mock<Func<int, Task>>();

            var invalid = new InvalidSessionException("invalid session");

            func.SetupSequence(f => f.Invoke())
                .ThrowsAsync(invalid)
                .ThrowsAsync(invalid)
                .ThrowsAsync(invalid)
                .ThrowsAsync(invalid)
                .ThrowsAsync(invalid)
                .ThrowsAsync(invalid)
                .ReturnsAsync(1);

            Assert.AreEqual(1, await handler.RetriableExecute<int>(func.Object,
                Driver.RetryPolicy.Builder().WithMaxRetries(4).Build(),
                recover.Object,
                retry.Object));

            func.Verify(f => f.Invoke(), Times.Exactly(7));
            recover.Verify(r => r.Invoke(), Times.Exactly(6));
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Exactly(6));
        }

        [TestMethod]
        public async Task RetriableExecute_BothLimitedAndUnlimitedRetryExceptions_UnlimitedRetriesShouldNotAffectRetryLimitCount()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance);

            var func = new Mock<Func<Task<int>>>();
            var recover = new Mock<Func<Task>>();
            var retry = new Mock<Func<int, Task>>();

            var occ = new OccConflictException("qldb");
            var invalid = new InvalidSessionException("invalid session");
            func.SetupSequence(f => f.Invoke())
                .ThrowsAsync(invalid)
                .ThrowsAsync(occ)
                .ThrowsAsync(invalid)
                .ThrowsAsync(occ)
                .ThrowsAsync(invalid)
                .ReturnsAsync(1);

            Assert.AreEqual(1, await handler.RetriableExecute<int>(func.Object,
                Driver.RetryPolicy.Builder().WithMaxRetries(4).Build(),
                recover.Object,
                retry.Object));

            func.Verify(f => f.Invoke(), Times.Exactly(6));
            recover.Verify(r => r.Invoke(), Times.Exactly(3));
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Exactly(5));
        }

        [TestMethod]
        public async Task RetriableExecute_RetryMoreThanLimit_ThrowTheLastException()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance);

            var func = new Mock<Func<Task<int>>>();
            var recover = new Mock<Func<Task>>();
            var retry = new Mock<Func<int, Task>>();

            var occ = new OccConflictException("qldb");
            var ase = new AmazonServiceException();
            var retriable = new RetriableException("testTransactionIdddddd", ase);
            func.SetupSequence(f => f.Invoke())
                .ThrowsAsync(retriable)
                .ThrowsAsync(occ)
                .ThrowsAsync(retriable)
                .ThrowsAsync(occ)
                .ThrowsAsync(retriable);

            Assert.AreEqual(ase,
                await Assert.ThrowsExceptionAsync<AmazonServiceException>(
                    () => handler.RetriableExecute(func.Object, Driver.RetryPolicy.Builder().Build(), recover.Object, retry.Object)));

            func.Verify(f => f.Invoke(), Times.Exactly(5));
            recover.Verify(r => r.Invoke(), Times.Never);
        }

        [TestMethod]
        public async Task RetriableExecute_CustomizedRetryPolicy_ThrowTheLastException()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(NullLogger.Instance);

            var func = new Mock<Func<Task<int>>>();
            var recover = new Mock<Func<Task>>();
            var retry = new Mock<Func<int, Task>>();

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
                await Assert.ThrowsExceptionAsync<AmazonServiceException>(
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
    }
}
