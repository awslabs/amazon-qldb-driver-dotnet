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

    /// <summary>
    /// The implementation of the session pool.
    /// </summary>
    internal class AsyncSessionPool : BaseSessionPool
    {
        private readonly BlockingCollection<AsyncQldbSession> sessionPool;
        private readonly Func<CancellationToken, Task<Session>> sessionCreator;
        private readonly IAsyncRetryHandler retryHandler;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncSessionPool"/> class.
        /// </summary>
        /// <param name="sessionCreator">
        /// The method to create a new underlying QLDB session. The operation can be cancelled.
        /// </param>
        /// <param name="retryHandler">Handling the retry logic of the execute call.</param>
        /// <param name="maxConcurrentTransactions">
        /// The maximum number of sessions that can be created from the pool at any one time.
        /// </param>
        /// <param name="logger">Logger to be used by this.</param>
        public AsyncSessionPool(
            Func<CancellationToken, Task<Session>> sessionCreator,
            IAsyncRetryHandler retryHandler,
            int maxConcurrentTransactions,
            ILogger logger)
            : base(maxConcurrentTransactions, logger)
        {
            this.sessionPool = new BlockingCollection<AsyncQldbSession>(maxConcurrentTransactions);
            this.sessionCreator = sessionCreator;
            this.retryHandler = retryHandler;
        }

        /// <summary>
        /// Execute a function asynchronously in session pool.
        /// </summary>
        /// <typeparam name="T">The return type of the function.</typeparam>
        ///
        /// <param name="func">The function to be executed in the session pool. The operation can be cancelled.</param>
        /// <param name="retryPolicy">The policy on retry.</param>
        /// <param name="cancellationToken">
        /// A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// <returns>The result from the function.</returns>
        public async Task<T> Execute<T>(
            Func<AsyncTransactionExecutor, Task<T>> func,
            RetryPolicy retryPolicy,
            CancellationToken cancellationToken = default)
        {
            AsyncQldbSession session = null;
            try
            {
                session = await this.GetSession(cancellationToken);
                return await this.retryHandler.RetriableExecute(
                    ct => session.Execute(func, ct),
                    retryPolicy,
                    async ct => session = await this.StartNewSession(ct),
                    async ct =>
                    {
                        this.poolPermits.Release();
                        session = await this.GetSession(ct);
                    },
                    cancellationToken);
            }
            finally
            {
                if (session != null)
                {
                    session.Release();
                }
            }
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

        /// <summary>
        /// <para>Get a <see cref="AsyncQldbSession"/> object.</para>
        ///
        /// <para>This will asynchronously attempt to retrieve an active existing session, or it will asynchronously start a new session with QLDB unless the
        /// number of allocated sessions has exceeded the pool size limit.</para>
        /// </summary>
        ///
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// <returns>The <see cref="AsyncQldbSession"/> object.</returns>
        ///
        /// <exception cref="QldbDriverException">Thrown when this driver has been disposed or timeout.</exception>
        internal async Task<AsyncQldbSession> GetSession(CancellationToken cancellationToken = default)
        {
            if (this.isClosed)
            {
                this.logger.LogError(ExceptionMessages.DriverClosed);
                throw new QldbDriverException(ExceptionMessages.DriverClosed);
            }

            this.logger.LogDebug(
                "Getting session. There are {} free sessions and {} available permits.",
                this.sessionPool.Count,
                this.sessionPool.BoundedCapacity - this.poolPermits.CurrentCount);

            if (await this.poolPermits.WaitAsync(DefaultTimeoutInMs, cancellationToken))
            {
                try
                {
                    var session = this.sessionPool.Count > 0 ? this.sessionPool.Take(cancellationToken) : null;

                    if (session == null)
                    {
                        session = await this.StartNewSession(cancellationToken);
                        this.logger.LogDebug("Creating new pooled session with ID {}.", session.GetSessionId());
                    }

                    return session;
                }
                catch (Exception e)
                {
                    this.poolPermits.Release();
                    throw e;
                }
            }
            else
            {
                this.logger.LogError(ExceptionMessages.SessionPoolEmpty);
                throw new QldbDriverException(ExceptionMessages.SessionPoolEmpty);
            }
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

        private async Task<AsyncQldbSession> StartNewSession(CancellationToken cancellationToken = default)
        {
            return new AsyncQldbSession(await this.sessionCreator(cancellationToken), this.ReleaseSession, this.logger);
        }
    }
}
