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
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;

    internal class AsyncSessionPool : BaseSessionPool
    {
        private readonly BlockingCollection<AsyncQldbSession> sessionPool;
        private readonly SemaphoreSlim poolPermits;
        private readonly Func<Session> sessionCreator;
        private readonly IAsyncRetryHandler retryHandler;
        private readonly ILogger logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncSessionPool"/> class.
        /// </summary>
        /// <param name="sessionCreator">The method to create a new underlying QLDB session.</param>
        /// <param name="retryHandler">Handling the retry logic of the execute call.</param>
        /// <param name="maxConcurrentTransactions">The maximum number of sessions that can be created from the pool at any one time.</param>
        /// <param name="logger">Logger to be used by this.</param>
        public AsyncSessionPool(Func<Session> sessionCreator, IAsyncRetryHandler retryHandler, int maxConcurrentTransactions, ILogger logger)
        {
            this.sessionPool = new BlockingCollection<AsyncQldbSession>(maxConcurrentTransactions);
            this.poolPermits = new SemaphoreSlim(maxConcurrentTransactions, maxConcurrentTransactions);
            this.sessionCreator = sessionCreator;
            this.retryHandler = retryHandler;
            this.logger = logger;
        }

        public Task<T> Execute<T>(Func<AsyncTransactionExecutor, Task> func, RetryPolicy retryPolicy, Action<int> retryAction)
        {
            throw new NotImplementedException();
        }

        internal AsyncQldbSession GetSession()
        {
            throw new NotImplementedException();
        }

        private void ReleaseSession(AsyncQldbSession session)
        {
            if (session != null && session.IsAlive())
            {
                this.sessionPool.Add(session);
            }

            this.poolPermits.Release();
            this.logger.LogDebug("Session returned to pool; pool size is now {}.", this.sessionPool.Count);
        }

        private AsyncQldbSession StartNewSession()
        {
            return new AsyncQldbSession(this.sessionCreator.Invoke(), this.ReleaseSession, this.logger);
        }

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
    }
}
