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
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.IonDotnet.Tree;
    using Amazon.IonDotnet.Tree.Impl;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class BufferedResultTests
    {
        private static readonly IValueFactory valueFactory = new ValueFactory();
        private static Mock<IResult> mockResult;
        private static List<IIonValue> testList;
        private static BufferedResult result;


        [ClassInitialize]
#pragma warning disable IDE0060 // Remove unused parameter
        public static async Task SetupClass(TestContext context)
#pragma warning restore IDE0060 // Remove unused parameter
        {
            mockResult = new Mock<IResult>();
            testList = new List<IIonValue>()
            {
                valueFactory.NewInt(0),
                valueFactory.NewInt(1),
                valueFactory.NewInt(2)
            };
            mockResult.Setup(x => x.GetAsyncEnumerator(It.IsAny<CancellationToken>())).Returns(new ValuesEnumerator(testList));

            result = await BufferedResult.BufferResult(mockResult.Object);
        }

        [TestMethod]
        public void TestBufferResultEnumeratesInput()
        {
            Assert.IsNotNull(result);
            mockResult.Verify(x => x.GetAsyncEnumerator(default), Times.Exactly(1));
        }

        [TestMethod]
        public async Task TestGetEnumeratorGetsAllInputEnumerableValues()
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

        private struct ValuesEnumerator : IAsyncEnumerator<IIonValue>
        {
            private List<IIonValue>.Enumerator valuesEnumerator;

            public ValuesEnumerator(List<IIonValue> values) => this.valuesEnumerator = values.GetEnumerator();

            public IIonValue Current => this.valuesEnumerator.Current;

            public ValueTask<bool> MoveNextAsync() => new ValueTask<bool>(this.valuesEnumerator.MoveNext());

            public ValueTask DisposeAsync() => default;
        }
    }
}
