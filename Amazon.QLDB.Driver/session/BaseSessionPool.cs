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
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// The base class for session pool.
    /// </summary>
    internal abstract class BaseSessionPool
    {
        protected const int DefaultTimeoutInMs = 1;
        protected readonly BlockingCollection<QldbSession> sessionPool;
        private readonly SemaphoreSlim poolPermits;
        protected readonly Func<Session> sessionCreator;
        protected readonly IRetryHandler retryHandler;
        protected readonly ILogger logger;
        protected bool isClosed = false;

        /// <summary>
        /// Dispose the session pool and all sessions.
        /// </summary>
        public void Dispose()
        {
            this.isClosed = true;
            while (this.sessionPool.Count > 0)
            {
                this.sessionPool.Take().Close();
            }
        }

        internal int AvailablePermit()
        {
            return this.poolPermits.CurrentCount;
        }
    }
}
