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
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.QLDBSession.Model;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class AsyncResultTests
    {
        private static AsyncResult result;
        private static Mock<Session> mockSession;
        private static readonly MemoryStream MemoryStream = new();
        private static readonly ValueHolder ValueHolder = new()
        {
            IonBinary = MemoryStream,
            IonText = "ionText"
        };
        private static readonly List<ValueHolder> ValueHolderList = new() { ValueHolder };

        private static readonly long ExecuteReads = 1;
        private static readonly long ExecuteWrites = 2;
        private static readonly long ExecuteTime = 100;
        private static readonly IOUsage ExecuteIO = new()
        {
            ReadIOs = ExecuteReads,
            WriteIOs = ExecuteWrites
        };
        private static readonly TimingInformation ExecuteTiming = new()
        {
            ProcessingTimeMilliseconds = ExecuteTime
        };
        private static readonly long FetchReads = 10;
        private static readonly long FetchWrites = 20;
        private static readonly long FetchTime = 1000;
        private static readonly IOUsage FetchIO = new()
        {
            ReadIOs = FetchReads,
            WriteIOs = FetchWrites
        };
        private static readonly TimingInformation FetchTiming = new()
        {
            ProcessingTimeMilliseconds = FetchTime
        };

        [TestInitialize]
        public void SetUp()
        {
            var executeResult = new ExecuteStatementResult
            {
                FirstPage = new Page
                {
                    NextPageToken = "hasNextPage",
                    Values = ValueHolderList
                }
            };

            mockSession = new Mock<Session>(null, null, null, null, null);
            result = new AsyncResult(mockSession.Object, "txnId", executeResult);
        }

        [TestMethod]
        public void TestGetAsyncEnumeratorAndThrowsExceptionWhenAlreadyRetrieved()
        {
            // First time is fine - sets a flag
            result.GetAsyncEnumerator();

            Assert.ThrowsException<InvalidOperationException>(() => result.GetAsyncEnumerator());
        }

        [TestMethod]
        public async Task TestAsyncMoveNextWithOneNextPage()
        {
            var ms = new MemoryStream();
            var valueHolderList = new List<ValueHolder> { new() { IonBinary = ms, IonText = "ionText" } };
            var fetchPageResult = new FetchPageResult
            {
                Page = new Page { NextPageToken = null, Values = valueHolderList }
            };
        
            mockSession.Setup(m => m.FetchPageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(fetchPageResult);
        
            var results = result.GetAsyncEnumerator();
        
            int counter = 0;
            while (await results.MoveNextAsync())
            {
                counter++;
            }
        
            Assert.AreEqual(2, counter);
            mockSession.Verify(m => m.FetchPageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);
        }
        
        [TestMethod]
        public async Task TestAsyncMoveNextWithNoNextPage()
        {
            var ms = new MemoryStream();
            var valueHolderList = new List<ValueHolder> { new() { IonBinary = ms, IonText = "ionText" } };
            var executeResult = new ExecuteStatementResult
            {
                FirstPage = new Page
                {
                    NextPageToken = null,
                    Values = valueHolderList
                }
            };
        
            AsyncResult res = new AsyncResult(mockSession.Object, "txnId", executeResult);
            var results = res.GetAsyncEnumerator();
        
            int counter = 0;
            while (await results.MoveNextAsync())
            {
                counter++;
            }
        
            Assert.AreEqual(1, counter);
            mockSession.Verify(m => m.FetchPageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        }
        
        [TestMethod]
        public async Task TestAsyncIonEnumeratorCurrentReturnsTrueWhenResultsExist()
        {
            var results = result.GetAsyncEnumerator();
        
            Assert.IsNotNull(results);
            Assert.IsTrue(await results.MoveNextAsync());
        }

        [TestMethod]
        public async Task TestAsyncQueryStatsNullExecuteNullFetch()
        {
            var executeResult = TestingUtilities.GetExecuteResultNullStats(ValueHolderList);
            var fetchPageResult = TestingUtilities.GetFetchResultNullStats(ValueHolderList);
        
            mockSession.Setup(m => m.FetchPageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(fetchPageResult);
        
            var result = new AsyncResult(mockSession.Object, "txnId", executeResult);
        
            var io = result.GetConsumedIOs();
            var timing = result.GetTimingInformation();
            Assert.IsNull(io);
            Assert.IsNull(timing);
        
            var results = result.GetAsyncEnumerator();
            while (await results.MoveNextAsync())
            {
                // Fetch the next page
            }
        
            io = result.GetConsumedIOs();
            timing = result.GetTimingInformation();
            Assert.IsNull(io);
            Assert.IsNull(timing);
        }
        
        [TestMethod]
        public async Task TestAsyncQueryStatsNullExecuteHasFetch()
        {
            var executeResult = TestingUtilities.GetExecuteResultNullStats(ValueHolderList);
            var fetchPageResult = TestingUtilities.GetFetchResultWithStats(ValueHolderList, FetchIO, FetchTiming);
        
            mockSession.Setup(m => m.FetchPageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(fetchPageResult);
        
            var result = new AsyncResult(mockSession.Object, "txnId", executeResult);
        
            var io = result.GetConsumedIOs();
            var timing = result.GetTimingInformation();
            Assert.IsNull(io);
            Assert.IsNull(timing);
        
            var results = result.GetAsyncEnumerator();
            while (await results.MoveNextAsync())
            {
                // Fetch the next page
            }
        
            io = result.GetConsumedIOs();
            timing = result.GetTimingInformation();
            Assert.AreEqual(FetchReads, io?.ReadIOs);
            Assert.AreEqual(FetchWrites, io?.WriteIOs);
            Assert.AreEqual(FetchTime, timing?.ProcessingTimeMilliseconds);
        }
        
        [TestMethod]
        public async Task TestAsyncQueryStatsHasExecuteNullFetch()
        {
            var executeResult = TestingUtilities.GetExecuteResultWithStats(ValueHolderList, ExecuteIO, ExecuteTiming);
            var fetchPageResult = TestingUtilities.GetFetchResultNullStats(ValueHolderList);
        
            mockSession.Setup(m => m.FetchPageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(fetchPageResult);
        
            var result = new AsyncResult(mockSession.Object, "txnId", executeResult);
        
            var io = result.GetConsumedIOs();
            var timing = result.GetTimingInformation();
            Assert.AreEqual(ExecuteReads, io?.ReadIOs);
            Assert.AreEqual(ExecuteWrites, io?.WriteIOs);
            Assert.AreEqual(ExecuteTime, timing?.ProcessingTimeMilliseconds);
        
            var results = result.GetAsyncEnumerator();
            while (await results.MoveNextAsync())
            {
                // Fetch the next page
            }
        
            io = result.GetConsumedIOs();
            timing = result.GetTimingInformation();
            Assert.AreEqual(ExecuteReads, io?.ReadIOs);
            Assert.AreEqual(ExecuteWrites, io?.WriteIOs);
            Assert.AreEqual(ExecuteTime, timing?.ProcessingTimeMilliseconds);
        }
        
        [TestMethod]
        public async Task TestAsyncQueryStatsHasExecuteHasFetch()
        {
            var executeResult = TestingUtilities.GetExecuteResultWithStats(ValueHolderList, ExecuteIO, ExecuteTiming);
            var fetchPageResult = TestingUtilities.GetFetchResultWithStats(ValueHolderList, FetchIO, FetchTiming);
        
            mockSession.Setup(m => m.FetchPageAsync(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(fetchPageResult);
        
            var result = new AsyncResult(mockSession.Object, "txnId", executeResult);
        
            var io = result.GetConsumedIOs();
            var timing = result.GetTimingInformation();
            Assert.AreEqual(ExecuteReads, io?.ReadIOs);
            Assert.AreEqual(ExecuteWrites, io?.WriteIOs);
            Assert.AreEqual(ExecuteTime, timing?.ProcessingTimeMilliseconds);
        
            var results = result.GetAsyncEnumerator();
            while (await results.MoveNextAsync())
            {
                // Fetch the next page
            }
        
            io = result.GetConsumedIOs();
            timing = result.GetTimingInformation();
            Assert.AreEqual(ExecuteReads + FetchReads, io?.ReadIOs);
            Assert.AreEqual(ExecuteWrites + FetchWrites, io?.WriteIOs);
            Assert.AreEqual(ExecuteTime + FetchTime, timing?.ProcessingTimeMilliseconds);
        }
    }
}
