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
        public static BufferedResult BufferResult(IResult result)
        {
            var values = new List<IIonValue>();
            foreach (IIonValue value in result)
            {
                values.Add(value);
            }

            return new BufferedResult(values);
        }

        /// <inheritdoc/>
        public IEnumerator<IIonValue> GetEnumerator()
        {
            return this.values.GetEnumerator();
        }

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }
}
