/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
    using IonDotnet.Tree;
    using Microsoft.Extensions.Logging;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class PooledQldbSessionTests
    {
        private static PooledQldbSession qldbSession;
        private static Mock<QldbSession> mockSession;
        private static Mock<MockDisposeDelegate> mockAction;
        private static Mock<ILogger> mockLogger;
        private static readonly Mock<IResult> mockResult = new Mock<IResult>();

        [TestInitialize]
        public void SetupClass()
        {
            mockSession = new Mock<QldbSession>(null, null, null);
            mockAction = new Mock<MockDisposeDelegate>();
            mockLogger = new Mock<ILogger>();
            qldbSession = new PooledQldbSession(mockSession.Object, mockAction.Object.DisposeDelegate, mockLogger.Object);
        }

        [TestMethod]
        public void TestConstructor()
        {
            Assert.IsNotNull(qldbSession);
        }

        [TestMethod]
        public void TestDispose()
        {
            qldbSession.Dispose();
            qldbSession.Dispose();
            mockAction.Verify(x => x.DisposeDelegate(mockSession.Object), Times.Exactly(1));
        }

        [TestMethod]
        public void TestExecuteStatement()
        {
            mockSession.Setup(x => x.Execute(It.IsAny<string>())).Returns(mockResult.Object);
            var result = qldbSession.Execute("testStatement");

            Assert.AreEqual(mockResult.Object, result);

            qldbSession.Dispose();
            Assert.ThrowsException<ObjectDisposedException>(() => qldbSession.Execute("testStatement"));
        }

        [TestMethod]
        public void TestExecuteAction()
        {
            static void testAction(TransactionExecutor executor) => executor.GetType();
            qldbSession.Execute(testAction);

            mockSession.Verify(x => x.Execute(It.IsAny<Action<TransactionExecutor>>()), Times.Exactly(1));

            qldbSession.Dispose();
            Assert.ThrowsException<ObjectDisposedException>(() => qldbSession.Execute(testAction));
        }

        [TestMethod]
        public void TestExecuteFunc()
        {
            static int testFunc(TransactionExecutor executor) { return 1; }
            mockSession.Setup(x => x.Execute(It.IsAny<Func<TransactionExecutor, int>>())).Returns(1);
            var result = qldbSession.Execute(testFunc);

            Assert.AreEqual(1, result);

            qldbSession.Dispose();
            Assert.ThrowsException<ObjectDisposedException>(() => qldbSession.Execute(testFunc));
        }

        [TestMethod]
        public void TestListTableNames()
        {
            var testList = new List<string>();
            mockSession.Setup(x => x.ListTableNames()).Returns(testList);

            Assert.AreEqual(testList, qldbSession.ListTableNames());

            qldbSession.Dispose();
            Assert.ThrowsException<ObjectDisposedException>(qldbSession.ListTableNames);
        }

        [TestMethod]
        public void TestStartTransaction()
        {
            var mockTransaction = new Mock<ITransaction>();
            mockSession.Setup(x => x.StartTransaction()).Returns(mockTransaction.Object);

            Assert.AreEqual(mockTransaction.Object, qldbSession.StartTransaction());

            qldbSession.Dispose();
            Assert.ThrowsException<ObjectDisposedException>(qldbSession.StartTransaction);
        }

        public class MockDisposeDelegate
        {
            public virtual void DisposeDelegate(IQldbSession session)
            {
            }
        }
    }
}
