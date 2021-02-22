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
    using System.Threading.Tasks;
    using System.Collections.Generic;
    using Amazon.IonDotnet.Tree;
    using Amazon.IonDotnet.Tree.Impl;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class AsyncTransactionExecutorTests
    {
        private static AsyncTransactionExecutor asyncTransactionExecutor;
        private static readonly string query = "my query";
        private static Mock<IAsyncResult> mockResult;
        private static Mock<AsyncTransaction> mockTransaction;

        [ClassInitialize]
#pragma warning disable IDE0060 // Remove unused parameter
        public static void Setup(TestContext context)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            mockResult = new Mock<IAsyncResult>();
            mockTransaction = new Mock<AsyncTransaction>(It.IsAny<Session>(), It.IsAny<string>(), null, null);
            mockTransaction.Setup(t => t.Abort()).Returns(Task.CompletedTask);
            mockTransaction.Setup(t => t.Execute(It.IsAny<string>())).ReturnsAsync(mockResult.Object);
            mockTransaction.Setup(t => t.Execute(It.IsAny<string>(), It.IsAny<List<IIonValue>>()))
                .ReturnsAsync(mockResult.Object);
            mockTransaction.Setup(t => t.Execute(It.IsAny<string>(), It.IsAny<IIonValue[]>()))
                .ReturnsAsync(mockResult.Object);
        }

        [TestInitialize]
        public void SetupTest()
        {
            asyncTransactionExecutor = new AsyncTransactionExecutor(mockTransaction.Object);
        }

        [TestMethod]
        [ExpectedException(typeof(TransactionAbortedException), AllowDerivedTypes = false)]
        public async Task TestAsyncAbortThrowsAbortException()
        {
            await asyncTransactionExecutor.Abort();
        }

        [TestMethod]
        public async Task TestAsyncExecuteNoParamsDelegatesCallToTransaction()
        {
            var actualResult = await asyncTransactionExecutor.Execute(query);
            mockTransaction.Verify(transaction => transaction.Execute(query), Times.Exactly(1));
            Assert.AreEqual(mockResult.Object, actualResult);
        }

        [TestMethod]
        public async Task TestAsyncExecuteEmptyParamsDelegatesCallToTransaction()
        {
            List<IIonValue> emptyParams = new List<IIonValue>();
            var actualResult = await asyncTransactionExecutor.Execute(query, emptyParams);
            mockTransaction.Verify(transaction => transaction.Execute(query, emptyParams), Times.Exactly(1));
            Assert.AreEqual(mockResult.Object, actualResult);
        }

        [TestMethod]
        public async Task TestAsyncExecuteWithNullParamsList()
        {
            List<IIonValue> nullParams = null;
            var actualResult = await asyncTransactionExecutor.Execute(query, nullParams);
            mockTransaction.Verify(transaction => transaction.Execute(query, (List<IIonValue>)null), Times.Exactly(1));
            Assert.AreEqual(mockResult.Object, actualResult);
        }

        [TestMethod]
        public async Task TestAsyncExecuteOneParamDelegatesCallToTransaction()
        {
            List<IIonValue> oneParam = new List<IIonValue> { new ValueFactory().NewInt(1) };
            var actualResult = await asyncTransactionExecutor.Execute(query, oneParam);
            mockTransaction.Verify(transaction => transaction.Execute(query, oneParam), Times.Exactly(1));
            Assert.AreEqual(mockResult.Object, actualResult);
        }

        [TestMethod]
        public async Task TestAsyncExecuteWithVarargs()
        {
            var ionFactory = new ValueFactory();
            IIonValue one = ionFactory.NewInt(1);
            IIonValue two = ionFactory.NewInt(2);
            var actualResult = await asyncTransactionExecutor.Execute(query, one, two);
            mockTransaction.Verify(transaction => transaction.Execute(query, one, two), Times.Exactly(1));
            Assert.AreEqual(mockResult.Object, actualResult);
        }
    }
}
