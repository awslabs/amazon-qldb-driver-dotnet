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
    using System.Threading;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Base class for Session Pool.
    /// </summary>
    internal abstract class BaseSessionPool
    {
        private protected const int DefaultTimeoutInMs = 1;
        private protected readonly SemaphoreSlim poolPermits;
        private protected readonly ILogger logger;
        private protected bool isClosed = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="BaseSessionPool"/> class.
        /// Abstract base constructor to initialize a new instance of a session pool.
        /// </summary>
        /// <param name="maxConcurrentTransactions">
        /// The maximum number of sessions that can be created from the pool at any one time.
        /// </param>
        /// <param name="logger">Logger to be used by this.</param>
        internal BaseSessionPool(int maxConcurrentTransactions, ILogger logger)
        {
            this.poolPermits = new SemaphoreSlim(maxConcurrentTransactions, maxConcurrentTransactions);
            this.logger = logger;
        }

        internal int AvailablePermit()
        {
            return this.poolPermits.CurrentCount;
        }
    }
}
