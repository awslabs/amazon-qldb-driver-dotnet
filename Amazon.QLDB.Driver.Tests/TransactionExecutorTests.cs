namespace Amazon.QLDB.Driver.Tests
{
    using System.Collections.Generic;
    using Amazon.IonDotnet.Tree;
    using Amazon.IonDotnet.Tree.Impl;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class TransactionExecutorTests
    {
        private static TransactionExecutor transactionExecutor;
        private static readonly string query = "my query";
        private static Mock<IResult> mockResult;
        private static Mock<Transaction> mockTransaction;

        [ClassInitialize]
#pragma warning disable IDE0060 // Remove unused parameter
        public static void Setup(TestContext context)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            mockResult = new Mock<IResult>();
            mockTransaction = new Mock<Transaction>(It.IsAny<Session>(), It.IsAny<string>(), null);
            mockTransaction.Setup(transaction => transaction.Execute(It.IsAny<string>())).Returns(mockResult.Object);
            mockTransaction.Setup(transaction => transaction.Execute(It.IsAny<string>(), It.IsAny<List<IIonValue>>()))
                .Returns(mockResult.Object);
            mockTransaction.Setup(transaction => transaction.Execute(It.IsAny<string>(), It.IsAny<IIonValue[]>()))
                .Returns(mockResult.Object);
        }

        [TestInitialize]
        public void SetupTest()
        {
            transactionExecutor = new TransactionExecutor(mockTransaction.Object);
        }

        [TestMethod]
        public void TestAbortThrowsAbortException()
        {
            Assert.ThrowsException<TransactionAbortedException>(transactionExecutor.Abort);
        }

        [TestMethod]
        public void TestExecuteNoParamsDelegatesCallToTransaction()
        {
            IResult actualResult = transactionExecutor.Execute(query);
            mockTransaction.Verify(transaction => transaction.Execute(query), Times.Exactly(1));
            Assert.AreEqual(mockResult.Object, actualResult);
        }

        [TestMethod]
        public void TestExecuteEmptyParamsDelegatesCallToTransaction()
        {
            List<IIonValue> emptyParams = new List<IIonValue>();
            IResult actualResult = transactionExecutor.Execute(query, emptyParams);
            mockTransaction.Verify(transaction => transaction.Execute(query, emptyParams), Times.Exactly(1));
            Assert.AreEqual(mockResult.Object, actualResult);
        }

        [TestMethod]
        public void TestExecuteWithNullParamsList()
        {
            List<IIonValue> nullParams = null;
            IResult actualResult = transactionExecutor.Execute(query, nullParams);
            mockTransaction.Verify(transaction => transaction.Execute(query, (List<IIonValue>)null), Times.Exactly(1));
            Assert.AreEqual(mockResult.Object, actualResult);
        }

        [TestMethod]
        public void TestExecuteOneParamDelegatesCallToTransaction()
        {
            List<IIonValue> oneParam = new List<IIonValue> { new ValueFactory().NewInt(1) };
            IResult actualResult = transactionExecutor.Execute(query, oneParam);
            mockTransaction.Verify(transaction => transaction.Execute(query, oneParam), Times.Exactly(1));
            Assert.AreEqual(mockResult.Object, actualResult);
        }

        [TestMethod]
        public void TestExecuteWithVarargs()
        {
            var ionFactory = new ValueFactory();
            IIonValue one = ionFactory.NewInt(1);
            IIonValue two = ionFactory.NewInt(2);
            IResult actualResult = transactionExecutor.Execute(query, one, two);
            mockTransaction.Verify(transaction => transaction.Execute(query, one, two), Times.Exactly(1));
            Assert.AreEqual(mockResult.Object, actualResult);
        }
    }
}
