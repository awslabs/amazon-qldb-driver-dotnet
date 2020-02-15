namespace Amazon.QLDB.Driver.Tests
{
    using System.Collections.Generic;
    using IonDotnet.Tree;
    using IonDotnet.Tree.Impl;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class TransactionExecutorTests
    {
        private static TransactionExecutor transactionExecutor;
        private static readonly string query = "my query";
        private static Mock<IResult> mockResult;
        private static Mock<ITransaction> mockTransaction;

        [ClassInitialize]
#pragma warning disable IDE0060 // Remove unused parameter
        public static void Setup(TestContext context)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            mockResult = new Mock<IResult>();
            mockTransaction = new Mock<ITransaction>();
            mockTransaction.Setup(transaction => transaction.Execute(It.IsAny<string>(), It.IsAny<List<IIonValue>>()))
                .Returns(mockResult.Object);
        }

        [TestInitialize]
        public void SetupTest()
        {
            transactionExecutor = new TransactionExecutor(mockTransaction.Object);
        }

        [TestMethod]
        public void TestAbort()
        {
            Assert.ThrowsException<AbortException>(transactionExecutor.Abort);
        }

        [TestMethod]
        public void TestExecuteNoParams()
        {
            IResult actualResult = transactionExecutor.Execute(query);
            mockTransaction.Verify(transaction => transaction.Execute(query, It.IsAny<List<IIonValue>>()), Times.Exactly(1));
            Assert.AreEqual(mockResult.Object, actualResult);
        }

        [TestMethod]
        public void TestExecuteEmptyParams()
        {
            List<IIonValue> emptyParams = new List<IIonValue>();
            IResult actualResult = transactionExecutor.Execute(query, emptyParams);
            mockTransaction.Verify(transaction => transaction.Execute(query, emptyParams), Times.Exactly(1));
            Assert.AreEqual(mockResult.Object, actualResult);
        }

        [TestMethod]
        public void TestExecuteOneParam()
        {
            List<IIonValue> oneParam = new List<IIonValue> { new ValueFactory().NewInt(1) };
            IResult actualResult = transactionExecutor.Execute(query, oneParam);
            mockTransaction.Verify(transaction => transaction.Execute(query, oneParam), Times.Exactly(1));
            Assert.AreEqual(mockResult.Object, actualResult);
        }
    }
}
