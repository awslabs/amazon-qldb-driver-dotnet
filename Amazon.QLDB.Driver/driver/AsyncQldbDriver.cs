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
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.QLDBSession;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// <para>Represents a factory for accessing a specific ledger within QLDB. This class or
    /// <see cref="QldbDriver"/> should be the main entry points to any interaction with QLDB.</para>
    ///
    /// <para>
    /// This factory pools sessions and attempts to return unused but available sessions when getting new sessions.
    /// The pool does not remove stale sessions until a new session is retrieved. The default pool size is the maximum
    /// amount of connections the session client allows set in the <see cref="ClientConfig"/>. <see cref="Dispose"/>
    /// should be called when this factory is no longer needed in order to clean up resources, ending all sessions in
    /// the pool.
    /// </para>
    /// </summary>
    public class AsyncQldbDriver : IAsyncQldbDriver
    {
        private readonly QldbDriverBase<AsyncQldbSession> driverBase;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncQldbDriver"/> class.
        /// </summary>
        ///
        /// <param name="ledgerName">The ledger to create sessions to.</param>
        /// <param name="sessionClient">AWS SDK session client for QLDB.</param>
        /// <param name="maxConcurrentTransactions">The maximum number of concurrent transactions.</param>
        /// <param name="logger">The logger to use.</param>
        /// <param name="serializer">The serializer to serialize and deserialize Ion data.</param>
        internal AsyncQldbDriver(
            string ledgerName,
            IAmazonQLDBSession sessionClient,
            int maxConcurrentTransactions,
            ILogger logger,
            ISerializer serializer)
        {
            this.driverBase =
                new QldbDriverBase<AsyncQldbSession>(ledgerName, sessionClient, maxConcurrentTransactions, logger, serializer);
        }

        /// <summary>
        /// Retrieve a builder object for creating a <see cref="AsyncQldbDriver"/>.
        /// </summary>
        ///
        /// <returns>The builder object for creating a <see cref="AsyncQldbDriver"/>.</returns>.
        public static AsyncQldbDriverBuilder Builder()
        {
            return new AsyncQldbDriverBuilder();
        }

        /// <summary>
        /// Close this driver and end all sessions in the current pool. No-op if already closed.
        /// </summary>
        public void Dispose()
        {
            this.driverBase.Dispose();
        }

        /// <inheritdoc/>
        public async Task Execute(
            Func<AsyncTransactionExecutor, Task> action,
            CancellationToken cancellationToken = default)
        {
            await this.Execute(action, QldbDriverBase<AsyncQldbSession>.DefaultRetryPolicy, cancellationToken);
        }

        /// <inheritdoc/>
        public async Task Execute(
            Func<AsyncTransactionExecutor, Task> action,
            RetryPolicy retryPolicy,
            CancellationToken cancellationToken = default)
        {
            await this.Execute(
                async txn =>
                {
                    await action.Invoke(txn);
                    return false;
                },
                retryPolicy,
                cancellationToken);
        }

        /// <inheritdoc/>
        public async Task<T> Execute<T>(
            Func<AsyncTransactionExecutor, Task<T>> func, CancellationToken cancellationToken = default)
        {
            return await this.Execute<T>(func, QldbDriverBase<AsyncQldbSession>.DefaultRetryPolicy, cancellationToken);
        }

        /// <inheritdoc/>
        public async Task<T> Execute<T>(
            Func<AsyncTransactionExecutor, Task<T>> func,
            RetryPolicy retryPolicy,
            CancellationToken cancellationToken = default)
        {
            this.driverBase.ThrowIfClosed();

            bool replaceDeadSession = false;
            for (int retryAttempt = 1; true; retryAttempt++)
            {
                AsyncQldbSession session = null;
                try
                {
                    if (replaceDeadSession)
                    {
                        session = await this.StartNewSession(cancellationToken);
                    }
                    else
                    {
                        session = await this.GetSession(cancellationToken);
                    }

                    T returnedValue = await session.Execute(func, cancellationToken);
                    this.driverBase.ReleaseSession(session);
                    return returnedValue;
                }
                catch (QldbTransactionException qte)
                {
                    replaceDeadSession = await this.driverBase.GetShouldReplaceDeadSessionOrThrowIfNoRetryAsync(
                        qte,
                        session,
                        retryAttempt,
                        retryPolicy,
                        cancellationToken);
                }
            }
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<string>> ListTableNames(CancellationToken cancellationToken = default)
        {
            IAsyncResult result = await this.Execute(
                async txn => await txn.Execute(QldbDriverBase<AsyncQldbSession>.TableNameQuery), cancellationToken);

            return (await result.ToListAsync(cancellationToken)).Select(i => i.StringValue);
        }

        internal async Task<AsyncQldbSession> GetSession(CancellationToken token)
        {
            return this.driverBase.GetSessionFromPool() ?? await this.StartNewSession(token);
        }

        private async Task<AsyncQldbSession> StartNewSession(CancellationToken token)
        {
            try
            {
                Session session = await Session.StartSessionAsync(
                    this.driverBase.LedgerName,
                    this.driverBase.SessionClient,
                    this.driverBase.Logger,
                    token);
                this.driverBase.Logger.LogDebug("Creating new pooled session with ID {}.", session.SessionId);
                return new AsyncQldbSession(session, this.driverBase.Logger, this.driverBase.Serializer);
            }
            catch (OperationCanceledException oce)
            {
                throw new QldbTransactionException(QldbTransactionException.DefaultTransactionId, false, oce);
            }
            catch (Exception e)
            {
                throw new RetriableException(QldbTransactionException.DefaultTransactionId, false, e);
            }
        }
    }
}
