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
    using Amazon.QLDBSession.Model;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    [TestClass]
    public class ResultTests
    {
        private static Result result;
        private static Mock<Session> mockSession;
        private static readonly MemoryStream MemoryStream = new MemoryStream();
        private static readonly ValueHolder ValueHolder = new ValueHolder
        {
            IonBinary = MemoryStream,
            IonText = "ionText"
        };
        private static readonly List<ValueHolder> ValueHolderList = new List<ValueHolder> { ValueHolder };

        private static readonly long ExecuteReads = 1;
        private static readonly long ExecuteWrites = 2;
        private static readonly long ExecuteTime = 100;
        private static readonly IOUsage ExecuteIO = new IOUsage
        {
            ReadIOs = ExecuteReads,
            WriteIOs = ExecuteWrites
        };
        private static readonly TimingInformation ExecuteTiming = new TimingInformation
        {
            ProcessingTimeMilliseconds = ExecuteTime
        };
        private static readonly long FetchReads = 10;
        private static readonly long FetchWrites = 20;
        private static readonly long FetchTime = 1000;
        private static readonly IOUsage FetchIO = new IOUsage
        {
            ReadIOs = FetchReads,
            WriteIOs = FetchWrites
        };
        private static readonly TimingInformation FetchTiming = new TimingInformation
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
            result = new Result(mockSession.Object, "txnId", executeResult);
        }

        [TestMethod]
        public void TestGetEnumeratorAndThrowsExceptionWhenAlreadyRetrieved()
        {
            // First time is fine - sets a flag
            result.GetEnumerator();

            Assert.ThrowsException<InvalidOperationException>(result.GetEnumerator);
        }

        [TestMethod]
        public void TestMoveNextWithOneNextPage()
        {
            var ms = new MemoryStream();
            var valueHolderList = new List<ValueHolder> { new ValueHolder { IonBinary = ms, IonText = "ionText" } };
            var fetchPageResult = new FetchPageResult
            {
                Page = new Page { NextPageToken = null, Values = valueHolderList }
            };

            mockSession.Setup(m => m.FetchPage(It.IsAny<string>(), It.IsAny<string>()))
                .Returns(fetchPageResult);

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
            var results = res.GetEnumerator();

            int counter = 0;
            while (results.MoveNext())
            {
                counter++;
            }

            Assert.AreEqual(1, counter);
            mockSession.Verify(m => m.FetchPage(It.IsAny<string>(), It.IsAny<string>()), Times.Never);
        }

        [TestMethod]
        public void TestIonEnumeratorCurrentReturnsTrueWhenResultsExist()
        {
            var results = result.GetEnumerator();

            Assert.IsNotNull(results);
            Assert.IsTrue(results.MoveNext());
        }

        [TestMethod]
        public void TestIonEnumeratorResetIsNotSupported()
        {
            var results = result.GetEnumerator();

            Assert.ThrowsException<NotSupportedException>(results.Reset);
        }

        [TestMethod]
        public void TestQueryStatsNullExecuteNullFetch()
        {
            var executeResult = TestingUtilities.GetExecuteResultNullStats(ValueHolderList);
            var fetchPageResult = TestingUtilities.GetFetchResultNullStats(ValueHolderList);

            mockSession.Setup(m => m.FetchPage(It.IsAny<string>(), It.IsAny<string>()))
                .Returns(fetchPageResult);

            var result = new Result(mockSession.Object, "txnId", executeResult);

            var io = result.GetConsumedIOs();
            var timing = result.GetTimingInformation();
            Assert.IsNull(io);
            Assert.IsNull(timing);

            var results = result.GetEnumerator();
            while (results.MoveNext())
            {
                // Fetch the next page
            }

            io = result.GetConsumedIOs();
            timing = result.GetTimingInformation();
            Assert.IsNull(io);
            Assert.IsNull(timing);
        }

        [TestMethod]
        public void TestQueryStatsNullExecuteHasFetch()
        {
            var executeResult = TestingUtilities.GetExecuteResultNullStats(ValueHolderList);
            var fetchPageResult = TestingUtilities.GetFetchResultWithStats(ValueHolderList, FetchIO, FetchTiming);

            mockSession.Setup(m => m.FetchPage(It.IsAny<string>(), It.IsAny<string>()))
                .Returns(fetchPageResult);

            var result = new Result(mockSession.Object, "txnId", executeResult);

            var io = result.GetConsumedIOs();
            var timing = result.GetTimingInformation();
            Assert.IsNull(io);
            Assert.IsNull(timing);

            var results = result.GetEnumerator();
            while (results.MoveNext())
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
        public void TestQueryStatsHasExecuteNullFetch()
        {
            var executeResult = TestingUtilities.GetExecuteResultWithStats(ValueHolderList, ExecuteIO, ExecuteTiming);
            var fetchPageResult = TestingUtilities.GetFetchResultNullStats(ValueHolderList);

            mockSession.Setup(m => m.FetchPage(It.IsAny<string>(), It.IsAny<string>()))
                .Returns(fetchPageResult);

            var result = new Result(mockSession.Object, "txnId", executeResult);

            var io = result.GetConsumedIOs();
            var timing = result.GetTimingInformation();
            Assert.AreEqual(ExecuteReads, io?.ReadIOs);
            Assert.AreEqual(ExecuteWrites, io?.WriteIOs);
            Assert.AreEqual(ExecuteTime, timing?.ProcessingTimeMilliseconds);

            var results = result.GetEnumerator();
            while (results.MoveNext())
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
        public void TestQueryStatsHasExecuteHasFetch()
        {
            var executeResult = TestingUtilities.GetExecuteResultWithStats(ValueHolderList, ExecuteIO, ExecuteTiming);
            var fetchPageResult = TestingUtilities.GetFetchResultWithStats(ValueHolderList, FetchIO, FetchTiming);

            mockSession.Setup(m => m.FetchPage(It.IsAny<string>(), It.IsAny<string>()))
                .Returns(fetchPageResult);

            var result = new Result(mockSession.Object, "txnId", executeResult);

            var io = result.GetConsumedIOs();
            var timing = result.GetTimingInformation();
            Assert.AreEqual(ExecuteReads, io?.ReadIOs);
            Assert.AreEqual(ExecuteWrites, io?.WriteIOs);
            Assert.AreEqual(ExecuteTime, timing?.ProcessingTimeMilliseconds);

            var results = result.GetEnumerator();
            while (results.MoveNext())
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
