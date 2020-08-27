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
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.IonDotnet.Tree;
    using Amazon.QLDBSession.Model;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class ResultTests
    {
        private static Result result;
        private static readonly Mock<Session> mockSession = new Mock<Session>(null, null, null, null, null);
        private readonly MemoryStream memoryStream = new MemoryStream();

        public IIonValue IonDatagram { get; private set; }

        [TestInitialize]
        public void SetUp()
        {
            var valueHolder = new ValueHolder { IonBinary = memoryStream, IonText = "ionText" };
            var valueHolderList = new List<ValueHolder> { valueHolder };
            var firstPage = new Page { NextPageToken = "hasNextPage", Values = valueHolderList };

            result = new Result(mockSession.Object, "txnId", firstPage);
        }

        [TestMethod]
        public void TestGetEnumeratorAndThrowsExceptionWhenAlreadyRetrieved()
        {
            // First time is fine - sets a flag
            result.GetAsyncEnumerator();

            Assert.ThrowsException<InvalidOperationException>(() => result.GetAsyncEnumerator());
        }

        [TestMethod]
        public async Task TestMoveNextWithOneNextPage()
        {
            var ms = new MemoryStream();
            var valuHolderList = new List<ValueHolder> { new ValueHolder { IonBinary = ms, IonText = "ionText" } };
            Page nextPage = new Page { NextPageToken = null, Values = valuHolderList };

            mockSession.Setup(m => m.FetchPage(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(new FetchPageResult { Page = nextPage });

            var results = result.GetAsyncEnumerator();

            int counter = 0;
            while (await results.MoveNextAsync())
            {
                counter++;
            }

            Assert.AreEqual(2, counter);
            mockSession.Verify(m => m.FetchPage(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);
        }

        [TestMethod]
        public async Task TestMoveNextWithNoNextPage()
        {
            Mock<Session> session = new Mock<Session>(null, null, null, null, null);
            var ms = new MemoryStream();
            var valuHolderList = new List<ValueHolder> { new ValueHolder { IonBinary = ms, IonText = "ionText" } };
            var firstPage = new Page { NextPageToken = null, Values = valuHolderList };

            Result res = new Result(session.Object, "txnId", firstPage);
            var results = res.GetAsyncEnumerator();

            int counter = 0;
            while (await results.MoveNextAsync())
            {
                counter++;
            }

            Assert.AreEqual(1, counter);
            session.Verify(m => m.FetchPage(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [TestMethod]
        public async Task TestIonEnumeratorCurrentReturnsTrueWhenResultsExist()
        {
            var results = result.GetAsyncEnumerator();

            Assert.IsNotNull(results);
            Assert.IsTrue(await results.MoveNextAsync());
        }
    }
}
