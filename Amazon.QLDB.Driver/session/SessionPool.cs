﻿/*
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
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// The implementation of the session pool.
    /// </summary>
    internal class SessionPool
    {
        private const int DEFAULTTIMEOUTMS = 1;
        private readonly BlockingCollection<QldbSession> sessionPool;
        private readonly SemaphoreSlim poolPermits;
        private readonly Func<Session> sessionCreator;
        private readonly int retryLimit;
        private readonly ILogger logger;
        private bool isClosed = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="SessionPool"/> class.
        /// </summary>
        /// <param name="sessionCreator">The method to create a new underlying QLDB session.</param>
        /// <param name="retryLimit">The limit for retries on execute methods when an OCC conflict or retriable exception occurs.</param>
        /// <param name="maxConcurrentTransactions">The maximum number of sessions that can be created from the pool at any one time.</param>
        /// <param name="logger">Logger to be used by this.</param>
        public SessionPool(Func<Session> sessionCreator, int retryLimit, int maxConcurrentTransactions, ILogger logger)
        {
            this.sessionPool = new BlockingCollection<QldbSession>(maxConcurrentTransactions);
            this.poolPermits = new SemaphoreSlim(maxConcurrentTransactions, maxConcurrentTransactions);
            this.sessionCreator = sessionCreator;
            this.retryLimit = retryLimit;
            this.logger = logger;
        }

        /// <summary>
        /// <para>Get a <see cref="IQldbSession"/> object.</para>
        ///
        /// <para>This will attempt to retrieve an active existing session, or it will start a new session with QLDB unless the
        /// number of allocated sessions has exceeded the pool size limit. If so, then it will continue trying to retrieve an
        /// active existing session until the timeout is reached, throwing a <see cref="TimeoutException"/>.</para>
        /// </summary>
        /// <returns>The <see cref="IQldbSession"/> object.</returns>
        ///
        /// <exception cref="QldbDriverException">Thrown when this driver has been disposed or timeout.</exception>
        public QldbSession GetSession()
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

            if (this.poolPermits.Wait(DEFAULTTIMEOUTMS))
            {
                try
                {
                    var session = this.sessionPool.Count > 0 ? this.sessionPool.Take() : null;

                    if (session == null)
                    {
                        session = this.StartNewSession();
                        this.logger.LogDebug("Creating new pooled session with ID {}.", session.GetSessionId());
                    }

                    return session.Renew();
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

        /// <summary>
        /// Dispose the session pool and all sessions.
        /// </summary>
        public void Dispose()
        {
            this.isClosed = true;
            while (this.sessionPool.Count > 0)
            {
                this.sessionPool.Take().Destroy();
            }
        }

        private void ReleaseSession(QldbSession session)
        {
            this.sessionPool.Add(session);
            this.poolPermits.Release();
            this.logger.LogDebug("Session returned to pool; pool size is now {}.", this.sessionPool.Count);
        }

        private QldbSession StartNewSession()
        {
            return new QldbSession(this.sessionCreator.Invoke(), this.retryLimit, this.ReleaseSession, this.logger);
        }
    }
}
