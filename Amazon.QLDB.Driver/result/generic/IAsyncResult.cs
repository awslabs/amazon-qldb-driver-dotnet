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

namespace Amazon.QLDB.Driver.Generic
{
    using System.Collections.Generic;

    /// <summary>
    /// Interface for the result of executing a statement in QLDB.
    /// Implements IEnumerable<T> to allow iteration over generic values of T within the result.
    ///
    /// Note that due to the fact that a result can only be retrieved from QLDB once, the IResult may only be
    /// iterated over once and is not thread-safe. Attempts to do so multiple times will result in an exception.
    /// </summary>
    public interface IAsyncResult<T> : IAsyncEnumerable<T>
    {
        /// <summary>
        /// Gets the current query statistics for the number of read IO requests. The statistics are stateful.
        /// </summary>
        ///
        /// <returns>The current IOUsage statistics.</returns>
        IOUsage? GetConsumedIOs();

        /// <summary>
        /// Gets the current query statistics for server-side processing time. The statistics are stateful.
        /// </summary>
        ///
        /// <returns>The current TimingInformation statistics.</returns>
        TimingInformation? GetTimingInformation();
    }
}
