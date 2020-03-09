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
        public void TestGetEnumeratorWhenRetrieved()
        {
            // First time is fine - sets a flag
            result.GetEnumerator();

            Assert.ThrowsException<InvalidOperationException>(result.GetEnumerator);
        }

        [TestMethod]
        public void TestMoveNextWithOneNextPage()
        {
            var ms = new MemoryStream();
            var valuHolderList = new List<ValueHolder> { new ValueHolder { IonBinary = ms, IonText = "ionText" } };
            Page nextPage = new Page { NextPageToken = null, Values = valuHolderList };

            mockSession.Setup(m => m.FetchPage(It.IsAny<string>(), It.IsAny<string>()))
                .Returns(new FetchPageResult { Page = nextPage });

            var results = result.GetEnumerator();

            int counter = 0;
            while (results.MoveNext())
            {
                counter++;
            }

            Assert.AreEqual(2, counter);
            mockSession.Verify(m => m.FetchPage(It.IsAny<string>(), It.IsAny<string>()), Times.Once);
        }

        [TestMethod]
        public void TestMoveNextWithNoNextPage()
        {
            Mock<Session> session = new Mock<Session>(null, null, null, null, null);
            var ms = new MemoryStream();
            var valuHolderList = new List<ValueHolder> { new ValueHolder { IonBinary = ms, IonText = "ionText" } };
            var firstPage = new Page { NextPageToken = null, Values = valuHolderList };

            Result res = new Result(session.Object, "txnId", firstPage);
            var results = res.GetEnumerator();

            int counter = 0;
            while (results.MoveNext())
            {
                counter++;
            }

            Assert.AreEqual(1, counter);
            session.Verify(m => m.FetchPage(It.IsAny<string>(), It.IsAny<string>()), Times.Never);
        }

        [TestMethod]
        public void TestIonEnumeratorCurrent()
        {
            var results = result.GetEnumerator();

            Assert.IsNotNull(results);
            Assert.IsTrue(results.MoveNext());
        }

        [TestMethod]
        public void TestIonEnumeratorReset()
        {
            var results = result.GetEnumerator();

            Assert.ThrowsException<NotSupportedException>(results.Reset);
        }
    }
}
