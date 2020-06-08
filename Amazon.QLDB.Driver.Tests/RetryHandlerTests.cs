
namespace Amazon.QLDB.Driver.Tests
{
    using System;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class RetryHandlerTests
    {
        [TestMethod]
        public void DoesNeedRecover_CheckIfNeedRecover_ShouldReplyCorrectValue()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(4);

            Assert.IsTrue(handler.IsRetriable(new OccConflictException("occ")));
            Assert.IsTrue(handler.IsRetriable(new RetriableException(new Exception())));
            Assert.IsTrue(handler.IsRetriable(new InvalidSessionException("invalid")));

            Assert.IsFalse(handler.IsRetriable(new AmazonQLDBSessionException("aqse")));
            Assert.IsFalse(handler.IsRetriable(new QldbDriverException("qldb")));

            Assert.IsFalse(handler.NeedsRecover(new OccConflictException("occ")));
            Assert.IsFalse(handler.NeedsRecover(new RetriableException(new Exception())));
            Assert.IsTrue(handler.NeedsRecover(new InvalidSessionException("invalid")));
        }

        [TestMethod]
        public void RetirableExecute_NoRetry_SuccessfulReturn()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(4);

            var func = new Mock<Func<int>>();
            var retry = new Mock<Action<int>>();
            var recover = new Mock<Action>();

            func.Setup(f => f.Invoke()).Returns(1);

            Assert.AreEqual(1, handler.RetriableExecute<int>(func.Object, retry.Object, recover.Object));

            func.Verify(f => f.Invoke(), Times.Once);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Never);
            recover.Verify(r => r.Invoke(), Times.Never);
        }

        [TestMethod]
        public void RetirableExecute_NotInListException_ThrowIt()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(4);

            var func = new Mock<Func<int>>();
            var retry = new Mock<Action<int>>();
            var recover = new Mock<Action>();

            var exception = new QldbDriverException("qldb");
            func.Setup(f => f.Invoke()).Throws(exception);

            Assert.AreEqual(exception,
                Assert.ThrowsException<QldbDriverException>(() => handler.RetriableExecute<int>(func.Object, retry.Object, recover.Object)));

            func.Verify(f => f.Invoke(), Times.Once);
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Never);
            recover.Verify(r => r.Invoke(), Times.Never);
        }

        [TestMethod]
        public void RetirableExecute_RetryWithoutRecoverWithinLimit_Succeed()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(4);

            var func = new Mock<Func<int>>();
            var retry = new Mock<Action<int>>();
            var recover = new Mock<Action>();

            var occ = new OccConflictException("qldb");
            var retriable = new RetriableException(new Exception());
            func.SetupSequence(f => f.Invoke()).Throws(occ).Throws(retriable).Throws(occ).Throws(retriable).Returns(1);

            Assert.AreEqual(1, handler.RetriableExecute<int>(func.Object, retry.Object, recover.Object));

            func.Verify(f => f.Invoke(), Times.Exactly(5));
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Exactly(4));
            recover.Verify(r => r.Invoke(), Times.Exactly(0));
        }

        [TestMethod]
        public void RetirableExecute_RetryWithRecoverWithinLimit_Succeed()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(4);

            var func = new Mock<Func<int>>();
            var retry = new Mock<Action<int>>();
            var recover = new Mock<Action>();

            var invalid = new InvalidSessionException("invalid session");
            var opened = new TransactionAlreadyOpenException(new Exception());
            func.SetupSequence(f => f.Invoke()).Throws(invalid).Throws(opened).Throws(invalid).Throws(opened).Returns(1);

            Assert.AreEqual(1, handler.RetriableExecute<int>(func.Object, retry.Object, recover.Object));

            func.Verify(f => f.Invoke(), Times.Exactly(5));
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Exactly(4));
            recover.Verify(r => r.Invoke(), Times.Exactly(4));
        }

        [TestMethod]
        public void RetirableExecute_RetryMoreThanLimit_ThrowTheLastException()
        {
            var handler = (RetryHandler)QldbDriverBuilder.CreateDefaultRetryHandler(4);

            var func = new Mock<Func<int>>();
            var retry = new Mock<Action<int>>();
            var recover = new Mock<Action>();

            var occ = new OccConflictException("qldb");
            var invalid = new InvalidSessionException("invalid session");
            func.SetupSequence(f => f.Invoke()).Throws(invalid).Throws(occ).Throws(invalid).Throws(occ).Throws(invalid);

            Assert.AreEqual(invalid,
                Assert.ThrowsException<InvalidSessionException>(() => handler.RetriableExecute<int>(func.Object, retry.Object, recover.Object)));

            func.Verify(f => f.Invoke(), Times.Exactly(5));
            retry.Verify(r => r.Invoke(It.IsAny<int>()), Times.Exactly(5));
            recover.Verify(r => r.Invoke(), Times.Exactly(3));
        }
    }
}
