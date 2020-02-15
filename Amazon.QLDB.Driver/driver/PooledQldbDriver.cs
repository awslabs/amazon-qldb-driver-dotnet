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
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Threading;
    using Amazon.QLDBSession;
    using Amazon.Runtime;
    using IonDotnet.Tree;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// <para>Represents a factory for accessing pooled sessions to a specific ledger within QLDB. This class or
    /// <see cref="QldbDriver"/> should be the main entry points to any interaction with QLDB. <see cref="GetSession"/> will create a
    /// <see cref="PooledQldbSession"/> to the specified ledger within QLDB as a communication channel. Any acquired sessions must
    /// be cleaned up with <see cref="PooledQldbSession.Dispose"/> when they are no longer needed in order to return the session to
    /// the pool. If this is not done, this driver may become unusable if the pool limit is exceeded.</para>
    ///
    /// <para>This factory pools sessions and attempts to return unused but available sessions when getting new sessions. The
    /// advantage to using this over the non-pooling driver is that the underlying connection that sessions use to
    /// communicate with QLDB can be recycled, minimizing resource usage by preventing unnecessary connections and reducing
    /// latency by not making unnecessary requests to start new connections and end reusable, existing, ones.</para>
    ///
    /// <para>The pool does not remove stale sessions until a new session is retrieved. The default pool size is the maximum
    /// amount of connections the session client allows set in the <see cref="ClientConfig"/>. <see cref="Dispose"/>
    /// should be called when this factory is no longer needed in order to clean up resources, ending all sessions in the pool.</para>
    /// </summary>
    public class PooledQldbDriver : IQldbDriver
    {
        private readonly string ledgerName;
        private readonly AmazonQLDBSessionClient sessionClient;
        private readonly int retryLimit;
        private readonly int timeout;
        private readonly ILogger logger;

        private readonly SemaphoreSlim poolPermits;
        private readonly BlockingCollection<QldbSession> sessionPool;
        private bool isClosed = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="PooledQldbDriver"/> class.
        /// </summary>
        /// <param name="ledgerName">The ledger to create sessions to.</param>
        /// <param name="sessionClient">QLDB session client.</param>
        /// <param name="retryLimit">The amount of retries sessions created by this driver will attempt upon encountering a non-fatal error.</param>
        /// <param name="poolLimit">The maximum number of sessions that can be created from the pool at any one time.</param>
        /// <param name="timeout">The maximum amount of time to wait, in milliseconds.</param>
        /// <param name="logger">Logger to be used by this.</param>
        internal PooledQldbDriver(
            string ledgerName,
            AmazonQLDBSessionClient sessionClient,
            int retryLimit,
            int poolLimit,
            int timeout,
            ILogger logger)
        {
            this.ledgerName = ledgerName;
            this.sessionClient = sessionClient;
            this.retryLimit = retryLimit;
            this.timeout = timeout;
            this.logger = logger;

            this.poolPermits = new SemaphoreSlim(poolLimit, poolLimit);
            this.sessionPool = new BlockingCollection<QldbSession>(poolLimit);
        }

        /// <summary>
        /// Retrieve a builder object for creating a <see cref="PooledQldbDriver"/>.
        /// </summary>
        /// <returns>The builder object for creating a <see cref="PooledQldbDriver"/>.</returns>.
        public static PooledQldbDriverBuilder Builder()
        {
            return new PooledQldbDriverBuilder();
        }

        /// <summary>
        /// Close this driver and end all sessions in the current pool. No-op if already closed.
        /// </summary>
        public void Dispose()
        {
            if (!this.isClosed)
            {
                this.isClosed = true;
                while (this.sessionPool.Count > 0)
                {
                    this.sessionPool.Take().Dispose();
                }
            }
        }

        /// <summary>
        /// Get a session, then execute the statement against QLDB and retrieve the result, and return the session to the pool.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="retryAction">A lambda that is invoked when the statement execution is about to be retried due to
        /// a retriable error. Can be null if not applicable.</param>
        /// <param name="parameters">Parameters to execute.</param>
        ///
        /// <returns>The result of executing the statement.</returns>
        ///
        /// <exception cref="ObjectDisposedException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonClientException">Thrown when there is an error executing against QLDB.</exception>
        /// <exception cref="TimeoutException">Thrown when no sessions were made available before the timeout.</exception>
        public IResult Execute(string statement, Action<int> retryAction = null, List<IIonValue> parameters = null)
        {
            using (var session = this.GetSession())
            {
                return session.Execute(statement, retryAction, parameters);
            }
        }

        /// <summary>
        /// Get a session, then execute the statement against QLDB and retrieve the result, and return the session to the pool.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="retryAction">A lambda that is invoked when the statement execution is about to be retried due to
        /// a retriable error. Can be null if not applicable.</param>
        /// <param name="parameters">Parameters to execute.</param>
        ///
        /// <returns>The result of executing the statement.</returns>
        ///
        /// <exception cref="ObjectDisposedException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonClientException">Thrown when there is an error executing against QLDB.</exception>
        /// <exception cref="TimeoutException">Thrown when no sessions were made available before the timeout.</exception>
        public IResult Execute(string statement, Action<int> retryAction = null, params IIonValue[] parameters)
        {
            using (var session = this.GetSession())
            {
                return session.Execute(statement, retryAction, parameters);
            }
        }

        /// <summary>
        /// Get a session, then execute the Executor lambda against QLDB within a transaction where no result is expected,
        /// and return the session to the pool.
        /// </summary>
        ///
        /// <param name="action">The Executor lambda with no return value representing the block of code to be executed within the transaction.
        /// This cannot have any side effects as it may be invoked multiple times.</param>
        /// <param name="retryAction">A lambda that is invoked when the Executor lambda is about to be retried due to
        /// a retriable error. Can be null if not applicable.</param>
        ///
        /// <exception cref="AbortException">Thrown if the Executor lambda calls <see cref="TransactionExecutor.Abort"/>.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonClientException">Thrown when there is an error executing against QLDB.</exception>
        /// <exception cref="TimeoutException">Thrown when no sessions were made available before the timeout.</exception>
        public void Execute(Action<TransactionExecutor> action, Action<int> retryAction = null)
        {
            using (var session = this.GetSession())
            {
                session.Execute(action, retryAction);
            }
        }

        /// <summary>
        /// Get a session, then execute the Executor lambda against QLDB and retrieve the result within a transaction,
        /// and return the session to the pool.
        /// </summary>
        ///
        /// <param name="func">The Executor lambda representing the block of code to be executed within the transaction. This cannot have any
        /// side effects as it may be invoked multiple times, and the result cannot be trusted until the
        /// transaction is committed.</param>
        /// <param name="retryAction">A lambda that is invoked when the Executor lambda is about to be retried due to
        /// a retriable error. Can be null if not applicable.</param>
        /// <typeparam name="T">The return type.</typeparam>
        ///
        /// <returns>The return value of executing the executor. Note that if you directly return a <see cref="IResult"/>, this will
        /// be automatically buffered in memory before the implicit commit to allow reading, as the commit will close
        /// any open results. Any other <see cref="IResult"/> instances created within the executor block will be
        /// invalidated, including if the return value is an object which nests said <see cref="IResult"/> instances within it.
        /// </returns>
        ///
        /// <exception cref="AbortException">Thrown if the Executor lambda calls <see cref="TransactionExecutor.Abort"/>.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonClientException">Thrown when there is an error executing against QLDB.</exception>
        /// <exception cref="TimeoutException">Thrown when no sessions were made available before the timeout.</exception>
        public T Execute<T>(Func<TransactionExecutor, T> func, Action<int> retryAction = null)
        {
            using (var session = this.GetSession())
            {
                return session.Execute(func, retryAction);
            }
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
        /// <exception cref="ObjectDisposedException">Thrown when this driver has been disposed.</exception>
        /// <exception cref="TimeoutException">Thrown when no sessions were made available before the timeout.</exception>
        public IQldbSession GetSession()
        {
            if (this.isClosed)
            {
                this.logger.LogError(ExceptionMessages.DriverClosed);
                throw new ObjectDisposedException(ExceptionMessages.DriverClosed);
            }

            this.logger.LogDebug(
                "Getting session. There are {} free sessions and {} available permits.",
                this.sessionPool.Count,
                this.sessionPool.BoundedCapacity - this.poolPermits.CurrentCount);

            if (this.poolPermits.Wait(this.timeout))
            {
                try
                {
                    while (this.sessionPool.Count > 0)
                    {
                        var session = this.sessionPool.Take();
                        if (session.AbortOrClose())
                        {
                            this.logger.LogDebug("Reusing session with ID {} from pool.", session.GetSessionId());
                            return this.WrapSession(session);
                        }
                    }

                    var newSession = this.StartNewSession();
                    this.logger.LogDebug("Creating new pooled session with ID {}.", newSession.GetSessionId());
                    return this.WrapSession(newSession);
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
                throw new TimeoutException(ExceptionMessages.SessionPoolEmpty);
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
            var newSession = Session.StartSession(this.ledgerName, this.sessionClient, this.logger);
            return new QldbSession(newSession, this.retryLimit, this.logger);
        }

        private PooledQldbSession WrapSession(QldbSession session)
        {
            return new PooledQldbSession(session, this.ReleaseSession, this.logger);
        }

        /// <summary>
        /// Builder object for creating a <see cref="PooledQldbDriver"/>, allowing for configuration of the parameters of
        /// construction.
        /// </summary>
        public class PooledQldbDriverBuilder : BaseQldbDriverBuilder<PooledQldbDriverBuilder, PooledQldbDriver>
        {
            private const int DefaultTimeout = 30000;

            private int poolLimit = 0;
            private int timeout = DefaultTimeout;

            /// <summary>
            /// Initializes a new instance of the <see cref="PooledQldbDriverBuilder"/> class.
            /// Use <see cref="Builder"/> to retrieve an instance of this class.
            /// </summary>
            internal PooledQldbDriverBuilder()
                : base()
            {
            }

            /// <summary>
            /// <para>Specify the limit to the pool of available sessions.</para>
            ///
            /// <para>Attempting to retrieve a session when the maximum number of sessions is already withdrawn will block until
            /// a session becomes available. Set to 0 by default to use the maximum possible amount allowed by the client
            /// builder's configuration.</para>
            /// </summary>
            ///
            /// <param name="poolLimit">
            /// The maximum number of sessions that can be created from the pool at any one time. This amount
            /// cannot exceed the amount set in the <see cref="Amazon.Runtime.ClientConfig"/> used for this builder.
            /// </param>
            /// <returns>This builder object.</returns>
            public PooledQldbDriverBuilder WithPoolLimit(int poolLimit)
            {
                ValidationUtils.AssertNotNegative(poolLimit, "poolLimit");
                this.poolLimit = poolLimit;
                return this.builderInstance;
            }

            /// <summary>
            /// <para>Specify the timeout to wait for an available session to return to the pool in milliseconds.</para>
            ///
            /// <para>Calling <see cref="GetSession"/> will wait until the timeout before throwing an exception if an available
            /// session is still not returned to the pool.</para>
            /// </summary>
            ///
            /// <param name="timeout">The maximum amount of time to wait, in milliseconds.</param>
            ///
            /// <returns>This builder object.</returns>
            public PooledQldbDriverBuilder WithTimeout(int timeout)
            {
                ValidationUtils.AssertNotNegative(timeout, "timeout");
                this.timeout = timeout;
                return this.builderInstance;
            }

            /// <summary>
            /// Creates the pooled driver.
            /// </summary>
            /// <returns>The driver object.</returns>
            internal override PooledQldbDriver ConstructDriver()
            {
                if (this.poolLimit == 0)
                {
                    this.poolLimit = (int)(this.SessionConfig.MaxConnectionsPerServer == null ?
                        int.MaxValue : this.SessionConfig.MaxConnectionsPerServer);
                }

                return new PooledQldbDriver(
                    this.LedgerName,
                    this.sessionClient,
                    this.RetryLimit,
                    this.poolLimit,
                    this.timeout,
                    this.Logger);
            }
        }
    }
}
