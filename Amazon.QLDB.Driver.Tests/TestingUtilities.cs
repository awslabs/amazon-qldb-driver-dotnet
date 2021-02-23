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
    using System.IO;
    using Amazon.IonDotnet.Builders;
    using Amazon.IonDotnet.Tree;
    using Amazon.IonDotnet.Utils;
    using Amazon.QLDBSession.Model;

    internal static class TestingUtilities
    {
        internal static ValueHolder CreateValueHolder(IIonValue ionValue)
        {
            MemoryStream stream = new MemoryStream();
            using (var writer = IonBinaryWriterBuilder.Build(stream))
            {
                ionValue.WriteTo(writer);
                writer.Finish();
            }

            var valueHolder = new ValueHolder
            {
                IonBinary = new MemoryStream(stream.GetWrittenBuffer()),
            };

            return valueHolder;
        }

        internal static ExecuteStatementResult GetExecuteResultNullStats(List<ValueHolder> valueHolderList)
        {
            return new ExecuteStatementResult
            {
                FirstPage = new Page
                {
                    NextPageToken = "hasNextPage",
                    Values = valueHolderList,
                },
            };
        }

        internal static ExecuteStatementResult GetExecuteResultWithStats(
            List<ValueHolder> valueHolderList,
            IOUsage executeIO,
            TimingInformation executeTiming)
        {
            return new ExecuteStatementResult
            {
                FirstPage = new Page
                {
                    NextPageToken = "hasNextPage",
                    Values = valueHolderList,
                },
                ConsumedIOs = executeIO,
                TimingInformation = executeTiming,
            };
        }

        internal static FetchPageResult GetFetchResultNullStats(List<ValueHolder> valueHolderList)
        {
            return new FetchPageResult
            {
                Page = new Page { NextPageToken = null, Values = valueHolderList },
            };
        }

        internal static FetchPageResult GetFetchResultWithStats(
            List<ValueHolder> valueHolderList,
            IOUsage fetchIO,
            TimingInformation fetchTiming)
        {
            return new FetchPageResult
            {
                Page = new Page { NextPageToken = null, Values = valueHolderList },
                ConsumedIOs = fetchIO,
                TimingInformation = fetchTiming,
            };
        }
    }
}
