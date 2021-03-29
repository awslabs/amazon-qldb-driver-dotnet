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

namespace Amazon.QLDB.Driver
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.IonDotnet.Tree;

    /// <summary>
    /// Implementation of a result which asynchronously buffers all values in memory, rather than stream them from QLDB
    /// during retrieval.
    /// This implementation should only be used when the result is to be returned after the parent transaction is to be
    /// committed.
    /// </summary>
    public class AsyncBufferedResult : BaseBufferedResult, IAsyncResult
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncBufferedResult"/> class.
        /// </summary>
        ///
        /// <param name="values">Buffer values.</param>
        /// <param name="consumedIOs">IOUsage statistics.</param>
        /// <param name="timingInformation">TimingInformation statistics.</param>
        private AsyncBufferedResult(List<IIonValue> values, IOUsage? consumedIOs, TimingInformation? timingInformation)
            : base(values, consumedIOs, timingInformation)
        {
        }

        /// <summary>
        /// Constructor for the result which asynchronously buffers into the memory the supplied result before closing
        /// it.
        /// </summary>
        ///
        /// <param name="result">The result which is to be buffered into memory and closed.</param>
        ///
        /// <returns>The <see cref="AsyncBufferedResult"/> object.</returns>
        public static async Task<AsyncBufferedResult> BufferResultAsync(IAsyncResult result)
        {
            var values = new List<IIonValue>();
            await foreach (IIonValue value in result)
            {
                values.Add(value);
            }

            return new AsyncBufferedResult(values, result.GetConsumedIOs(), result.GetTimingInformation());
        }

        /// <summary>
        /// Returns an enumerator that iterates through a collection.
        /// </summary>
        ///
        /// <param name="cancellationToken">Cancellation token.</param>
        ///
        /// <returns>
        /// An <see cref="IAsyncEnumerator"/> object that can be used to iterate through the collection.
        /// </returns>
        public IAsyncEnumerator<IIonValue> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return new ValuesAsyncEnumerator(this.values);
        }

        /// <summary>
        /// Asynchronously enumerates a list of Ion values.
        /// </summary>
        internal struct ValuesAsyncEnumerator : IAsyncEnumerator<IIonValue>
        {
            private List<IIonValue>.Enumerator valuesEnumerator;

            public ValuesAsyncEnumerator(List<IIonValue> values) => this.valuesEnumerator = values.GetEnumerator();

            public IIonValue Current => this.valuesEnumerator.Current;

            public ValueTask<bool> MoveNextAsync() => new (this.valuesEnumerator.MoveNext());

            public ValueTask DisposeAsync() => default;
        }
    }
}
