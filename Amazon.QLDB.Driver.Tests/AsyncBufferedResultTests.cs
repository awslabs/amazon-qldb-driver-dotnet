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
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.IonDotnet.Tree;
    using Amazon.IonDotnet.Tree.Impl;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;
    using static Amazon.QLDB.Driver.AsyncBufferedResult;

    [TestClass]
    public class AsyncBufferedResultTests
    {
        private static readonly IValueFactory valueFactory = new ValueFactory();
        private static Mock<IAsyncResult> mockAsyncResult;
        private static List<IIonValue> testList;
        private static IOUsage testIO;
        private static TimingInformation testTiming;
        private static AsyncBufferedResult result;


        [ClassInitialize]
#pragma warning disable IDE0060 // Remove unused parameter
        public static async Task SetupClass(TestContext context)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            mockAsyncResult = new Mock<IAsyncResult>();
            testList = new List<IIonValue>()
            {
                valueFactory.NewInt(0),
                valueFactory.NewInt(1),
                valueFactory.NewInt(2)
            };
            testIO = new IOUsage(1, 2);
            testTiming = new TimingInformation(100);
            mockAsyncResult.Setup(x => x.GetAsyncEnumerator(It.IsAny<CancellationToken>()))
                .Returns(new ValuesAsyncEnumerator(testList));
            mockAsyncResult.Setup(x => x.GetConsumedIOs()).Returns(testIO);
            mockAsyncResult.Setup(x => x.GetTimingInformation()).Returns(testTiming);

            result = await BufferResultAsync(mockAsyncResult.Object);
        }

        [TestMethod]
        public void TestAsyncBufferResultEnumeratesInput()
        {
            Assert.IsNotNull(result);
            mockAsyncResult.Verify(x => x.GetAsyncEnumerator(It.IsAny<CancellationToken>()),
                Times.Exactly(1));
        }

        [TestMethod]
        public void TestAsyncBufferResultGetsMetrics()
        {
            Assert.AreEqual(testIO, result.GetConsumedIOs());
            Assert.AreEqual(testTiming, result.GetTimingInformation());
            mockAsyncResult.Verify(x => x.GetConsumedIOs(), Times.Exactly(1));
            mockAsyncResult.Verify(x => x.GetTimingInformation(), Times.Exactly(1));
        }

        [TestMethod]
        public async Task TestGetAsyncEnumeratorGetsAllInputEnumerableValues()
        {
            int count = 0;
            var enumerator = result.GetAsyncEnumerator();
            while (await enumerator.MoveNextAsync())
            {
                Assert.AreEqual(count, enumerator.Current.IntValue);
                count++;
            }
            Assert.AreEqual(testList.Count, count);
        }
    }
}
