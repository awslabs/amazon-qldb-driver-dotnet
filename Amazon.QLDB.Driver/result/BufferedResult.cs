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
    using System.Collections;
    using System.Collections.Generic;
    using Amazon.IonDotnet.Tree;

    /// <summary>
    /// Implementation of a result which buffers all values in memory, rather than stream them from QLDB during retrieval.
    /// This implementation should only be used when the result is to be returned after the parent transaction is to be
    /// committed.
    /// </summary>
    public class BufferedResult : IResult
    {
        private readonly List<IIonValue> values;
        private readonly IOUsage? consumedIOs;
        private readonly TimingInformation? timingInformation;

        /// <summary>
        /// Prevents a default instance of the <see cref="BufferedResult"/> class from being created.
        /// </summary>
        ///
        /// <param name="values">Buffer values.</param>
        /// <param name="consumedIOs">IOUsage statistics.</param>
        /// <param name="timingInformation">TimingInformation statistics.</param>
        private BufferedResult(List<IIonValue> values, IOUsage? consumedIOs, TimingInformation? timingInformation)
        {
            this.values = values;
            this.consumedIOs = consumedIOs;
            this.timingInformation = timingInformation;
        }

        /// <summary>
        /// Constructor for the result which buffers into the memory the supplied result before closing it.
        /// </summary>
        ///
        /// <param name="result">The result which is to be buffered into memory and closed.</param>
        ///
        /// <returns>The <see cref="BufferedResult"/> object.</returns>
        public static BufferedResult BufferResult(IResult result)
        {
            var values = new List<IIonValue>();
            foreach (IIonValue value in result)
            {
                values.Add(value);
            }

            return new BufferedResult(values, result.GetConsumedIOs(), result.GetTimingInformation());
        }

        /// <summary>
        /// Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns>An <see cref="IEnumerator"/> object that can be used to iterate through the collection.</returns>
        public IEnumerator<IIonValue> GetEnumerator()
        {
            return this.values.GetEnumerator();
        }

        /// <summary>
        /// Returns an enumerator that iterates through a collection.
        /// </summary>
        /// <returns>An <see cref="IEnumerator"/> object that can be used to iterate through the collection.</returns>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }

        /// <summary>
        /// Gets the current query statistics for the number of read IO requests. The statistics are stateful.
        /// </summary>
        ///
        /// <returns>The current IOUsage statistics.</returns>
        public IOUsage? GetConsumedIOs()
        {
            return this.consumedIOs;
        }

        /// <summary>
        /// Gets the current query statistics for server-side processing time. The statistics are stateful.
        /// </summary>
        ///
        /// <returns>The current TimingInformation statistics.</returns>
        public TimingInformation? GetTimingInformation()
        {
            return this.timingInformation;
        }
    }
}
