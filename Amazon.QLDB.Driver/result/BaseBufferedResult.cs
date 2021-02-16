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
    using Amazon.IonDotnet.Tree;

    /// <summary>
    /// Base class for Buffered Result.
    /// </summary>
    public abstract class BaseBufferedResult
    {
        private protected readonly List<IIonValue> values;
        private protected readonly IOUsage? consumedIOs;
        private protected readonly TimingInformation? timingInformation;

        /// <summary>
        /// Initializes a new instance of the <see cref="BaseBufferedResult"/> class.
        /// </summary>
        ///
        /// <param name="values">Buffer values.</param>
        /// <param name="consumedIOs">IOUsage statistics.</param>
        /// <param name="timingInformation">TimingInformation statistics.</param>
        private protected BaseBufferedResult(
            List<IIonValue> values,
            IOUsage? consumedIOs,
            TimingInformation? timingInformation)
        {
            this.values = values;
            this.consumedIOs = consumedIOs;
            this.timingInformation = timingInformation;
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
