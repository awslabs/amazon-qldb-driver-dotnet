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

namespace Amazon.QLDB.Driver
{
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.IonDotnet.Tree;

    /// <summary>
    /// Implementation of a result which buffers all values in memory, rather than stream them from QLDB during retrieval.
    /// This implementation should only be used when the result is to be returned after the parent transaction is to be
    /// committed.
    /// </summary>
    public class BufferedResult : IResult
    {
        private readonly List<IIonValue> values;

        /// <summary>
        /// Prevents a default instance of the <see cref="BufferedResult"/> class from being created.
        /// </summary>
        ///
        /// <param name="values">Buffer values.</param>
        private BufferedResult(List<IIonValue> values)
        {
            this.values = values;
        }

        /// <summary>
        /// Constructor for the result which buffers into the memory the supplied result before closing it.
        /// </summary>
        ///
        /// <param name="result">The result which is to be buffered into memory and closed.</param>
        ///
        /// <returns>The <see cref="BufferedResult"/> object.</returns>
        public static async Task<BufferedResult> BufferResult(IResult result)
        {
            var values = new List<IIonValue>();
            await foreach (IIonValue value in result)
            {
                values.Add(value);
            }

            return new BufferedResult(values);
        }

        /// <summary>
        /// Returns an enumerator that iterates through a collection.
        /// </summary>
        ///
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// <returns>An <see cref="IAsyncEnumerator&lt;IIonValue&gt;"/> object that can be used to iterate through the collection.</returns>
        public IAsyncEnumerator<IIonValue> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            return new ValuesEnumerator(this.values);
        }

        /// <summary>
        /// Asynchronously enumerates and list of Ion values.
        /// </summary>
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
