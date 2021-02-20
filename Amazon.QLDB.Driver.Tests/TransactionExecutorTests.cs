namespace Amazon.QLDB.Driver.Tests
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
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
        private static Mock<ITransaction> mockTransaction;

        [ClassInitialize]
#pragma warning disable IDE0060 // Remove unused parameter
        public static void Setup(TestContext context)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            mockResult = new Mock<IResult>();
            mockTransaction = new Mock<ITransaction>();
            mockTransaction.Setup(transaction => transaction.Execute(It.IsAny<string>(), It.IsAny<CancellationToken>())).ReturnsAsync(mockResult.Object);
            mockTransaction.Setup(transaction => transaction.Execute(It.IsAny<string>(), It.IsAny<List<IIonValue>>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(mockResult.Object);
            mockTransaction.Setup(transaction => transaction.Execute(It.IsAny<string>(), It.IsAny<CancellationToken>(), It.IsAny<IIonValue[]>()))
                .ReturnsAsync(mockResult.Object);
        }

        [TestInitialize]
        public void SetupTest()
        {
            transactionExecutor = new TransactionExecutor(mockTransaction.Object);
        }

        [TestMethod]
        public async Task TestAbortThrowsAbortException()
        {
            await Assert.ThrowsExceptionAsync<TransactionAbortedException>(() => transactionExecutor.Abort());
        }

        [TestMethod]
        public async Task TestExecuteNoParamsDelegatesCallToTransaction()
        {
            IResult actualResult = await transactionExecutor.Execute(query);
            mockTransaction.Verify(transaction => transaction.Execute(query, It.IsAny<CancellationToken>()), Times.Exactly(1));
            Assert.AreEqual(mockResult.Object, actualResult);
        }

        [TestMethod]
        public async Task TestExecuteEmptyParamsDelegatesCallToTransaction()
        {
            List<IIonValue> emptyParams = new List<IIonValue>();
            IResult actualResult = await transactionExecutor.Execute(query, emptyParams);
            mockTransaction.Verify(transaction => transaction.Execute(query, emptyParams, It.IsAny<CancellationToken>()), Times.Exactly(1));
            Assert.AreEqual(mockResult.Object, actualResult);
        }

        [TestMethod]
        public async Task TestExecuteWithNullParamsList()
        {
            List<IIonValue> nullParams = null;
            IResult actualResult = await transactionExecutor.Execute(query, nullParams);
            mockTransaction.Verify(transaction => transaction.Execute(query, (List<IIonValue>)null, It.IsAny<CancellationToken>()), Times.Exactly(1));
            Assert.AreEqual(mockResult.Object, actualResult);
        }

        [TestMethod]
        public async Task TestExecuteOneParamDelegatesCallToTransaction()
        {
            List<IIonValue> oneParam = new List<IIonValue> { new ValueFactory().NewInt(1) };
            IResult actualResult = await transactionExecutor.Execute(query, oneParam);
            mockTransaction.Verify(transaction => transaction.Execute(query, oneParam, It.IsAny<CancellationToken>()), Times.Exactly(1));
            Assert.AreEqual(mockResult.Object, actualResult);
        }

        [TestMethod]
        public async Task TestExecuteWithVarargs()
        {
            var ionFactory = new ValueFactory();
            IIonValue one = ionFactory.NewInt(1);
            IIonValue two = ionFactory.NewInt(2);
            IResult actualResult = await transactionExecutor.Execute(query, one, two);
            mockTransaction.Verify(transaction => transaction.Execute(query, It.IsAny<CancellationToken>(), one, two), Times.Exactly(1));
            Assert.AreEqual(mockResult.Object, actualResult);
        }
    }
}
