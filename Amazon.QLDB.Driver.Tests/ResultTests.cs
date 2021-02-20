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
    using Amazon.IonDotnet.Tree;
    using Amazon.QLDBSession.Model;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class ResultTests
    {
        private static Result result;
        private static Mock<Session> mockSession;
        private static readonly MemoryStream memoryStream = new MemoryStream();
        private static readonly ValueHolder valueHolder = new ValueHolder
        {
            IonBinary = memoryStream,
            IonText = "ionText"
        };
        private static readonly List<ValueHolder> valueHolderList = new List<ValueHolder> { valueHolder };

        private static readonly long executeReads = 1;
        private static readonly long executeWrites = 2;
        private static readonly long executeTime = 100;
        private static readonly IOUsage executeIO = new IOUsage
        {
            ReadIOs = executeReads,
            WriteIOs = executeWrites
        };
        private static readonly TimingInformation executeTiming = new TimingInformation
        {
            ProcessingTimeMilliseconds = executeTime
        };
        private static readonly long fetchReads = 10;
        private static readonly long fetchWrites = 20;
        private static readonly long fetchTime = 1000;
        private static readonly IOUsage fetchIO = new IOUsage
        {
            ReadIOs = fetchReads,
            WriteIOs = fetchWrites
        };
        private static readonly TimingInformation fetchTiming = new TimingInformation
        {
            ProcessingTimeMilliseconds = fetchTime
        };

        [TestInitialize]
        public void SetUp()
        {
            var executeResult = new ExecuteStatementResult
            {
                FirstPage = new Page
                {
                    NextPageToken = "hasNextPage",
                    Values = valueHolderList
                }
            };

            mockSession = new Mock<Session>(null, null, null, null, null);
            result = new Result(mockSession.Object, "txnId", executeResult);
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
            var valueHolderList = new List<ValueHolder> { new ValueHolder { IonBinary = ms, IonText = "ionText" } };
            var fetchPageResult = new FetchPageResult
            {
                Page = new Page { NextPageToken = null, Values = valueHolderList }
            };

            mockSession.Setup(m => m.FetchPage(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(fetchPageResult);

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
            var ms = new MemoryStream();
            var valueHolderList = new List<ValueHolder> { new ValueHolder { IonBinary = ms, IonText = "ionText" } };
            var executeResult = new ExecuteStatementResult
            {
                FirstPage = new Page
                {
                    NextPageToken = null,
                    Values = valueHolderList
                }
            };

            Result res = new Result(mockSession.Object, "txnId", executeResult);
            var results = res.GetAsyncEnumerator();

            int counter = 0;
            while (await results.MoveNextAsync())
            {
                counter++;
            }

            Assert.AreEqual(1, counter);
            mockSession.Verify(m => m.FetchPage(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Never);
        }

        [TestMethod]
        public async Task TestIonEnumeratorCurrentReturnsTrueWhenResultsExist()
        {
            var results = result.GetAsyncEnumerator();

            Assert.IsNotNull(results);
            Assert.IsTrue(await results.MoveNextAsync());
        }

        [TestMethod]
        public async Task TestQueryStatsNullExecuteNullFetch()
        {
            var executeResult = GetExecuteResultNullStats();
            var fetchPageResult = GetFetchResultNullStats();

            mockSession.Setup(m => m.FetchPage(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(fetchPageResult);

            var result = new Result(mockSession.Object, "txnId", executeResult);

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
        public async Task TestQueryStatsNullExecuteHasFetch()
        {
            var executeResult = GetExecuteResultNullStats();
            var fetchPageResult = GetFetchResultWithStats();

            mockSession.Setup(m => m.FetchPage(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(fetchPageResult);

            var result = new Result(mockSession.Object, "txnId", executeResult);

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
            Assert.AreEqual(fetchReads, io?.ReadIOs);
            Assert.AreEqual(fetchWrites, io?.WriteIOs);
            Assert.AreEqual(fetchTime, timing?.ProcessingTimeMilliseconds);
        }

        [TestMethod]
        public async Task TestQueryStatsHasExecuteNullFetch()
        {
            var executeResult = GetExecuteResultWithStats();
            var fetchPageResult = GetFetchResultNullStats();

            mockSession.Setup(m => m.FetchPage(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(fetchPageResult);

            var result = new Result(mockSession.Object, "txnId", executeResult);

            var io = result.GetConsumedIOs();
            var timing = result.GetTimingInformation();
            Assert.AreEqual(executeReads, io?.ReadIOs);
            Assert.AreEqual(executeWrites, io?.WriteIOs);
            Assert.AreEqual(executeTime, timing?.ProcessingTimeMilliseconds);

            var results = result.GetAsyncEnumerator();
            while (await results.MoveNextAsync())
            {
                // Fetch the next page
            }

            io = result.GetConsumedIOs();
            timing = result.GetTimingInformation();
            Assert.AreEqual(executeReads, io?.ReadIOs);
            Assert.AreEqual(executeWrites, io?.WriteIOs);
            Assert.AreEqual(executeTime, timing?.ProcessingTimeMilliseconds);
        }

        [TestMethod]
        public async Task TestQueryStatsHasExecuteHasFetch()
        {
            var executeResult = GetExecuteResultWithStats();
            var fetchPageResult = GetFetchResultWithStats();

            mockSession.Setup(m => m.FetchPage(It.IsAny<string>(), It.IsAny<string>(), It.IsAny<CancellationToken>()))
                .ReturnsAsync(fetchPageResult);

            var result = new Result(mockSession.Object, "txnId", executeResult);

            var io = result.GetConsumedIOs();
            var timing = result.GetTimingInformation();
            Assert.AreEqual(executeReads, io?.ReadIOs);
            Assert.AreEqual(executeWrites, io?.WriteIOs);
            Assert.AreEqual(executeTime, timing?.ProcessingTimeMilliseconds);

            var results = result.GetAsyncEnumerator();
            while (await results.MoveNextAsync())
            {
                // Fetch the next page
            }

            io = result.GetConsumedIOs();
            timing = result.GetTimingInformation();
            Assert.AreEqual(executeReads + fetchReads, io?.ReadIOs);
            Assert.AreEqual(executeWrites + fetchWrites, io?.WriteIOs);
            Assert.AreEqual(executeTime + fetchTime, timing?.ProcessingTimeMilliseconds);
        }

        private ExecuteStatementResult GetExecuteResultNullStats()
        {
            return new ExecuteStatementResult
            {
                FirstPage = new Page
                {
                    NextPageToken = "hasNextPage",
                    Values = valueHolderList
                }
            };
        }

        private ExecuteStatementResult GetExecuteResultWithStats()
        {
            return new ExecuteStatementResult
            {
                FirstPage = new Page
                {
                    NextPageToken = "hasNextPage",
                    Values = valueHolderList
                },
                ConsumedIOs = executeIO,
                TimingInformation = executeTiming
            };
        }

        private FetchPageResult GetFetchResultNullStats()
        {
            return new FetchPageResult
            {
                Page = new Page { NextPageToken = null, Values = valueHolderList }
            };
        }

        private FetchPageResult GetFetchResultWithStats()
        {
            return new FetchPageResult
            {
                Page = new Page { NextPageToken = null, Values = valueHolderList },
                ConsumedIOs = fetchIO,
                TimingInformation = fetchTiming
            };
        }
    }
}
