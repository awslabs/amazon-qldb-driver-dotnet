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

    /// <summary>
    /// <para>Represents a factory for accessing a specific ledger within QLDB. This class or
    /// <see cref="QldbDriver"/> should be the main entry points to any interaction with QLDB.</para>
    ///
    /// <para>This factory pools sessions and attempts to return unused but available sessions when getting new sessions.
    /// The pool does not remove stale sessions until a new session is retrieved. The default pool size is the maximum
    /// amount of connections the session client allows set in the <see cref="ClientConfig"/>. <see cref="Dispose"/>
    /// should be called when this factory is no longer needed in order to clean up resources, ending all sessions in the pool.</para>
    /// </summary>
    public class AsyncQldbDriver : BaseQldbDriver, IAsyncQldbDriver
    {
        private readonly AsyncSessionPool sessionPool;

        internal AsyncQldbDriver(AsyncSessionPool sessionPool)
        {
            this.sessionPool = sessionPool;
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
            this.sessionPool.Dispose();
        }

        /// <inheritdoc/>
        public async Task Execute(
            Func<AsyncTransactionExecutor, Task> action,
            CancellationToken cancellationToken = default)
        {
            await this.Execute(action, BaseQldbDriver.DefaultRetryPolicy, cancellationToken);
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
            return await this.Execute<T>(func, BaseQldbDriver.DefaultRetryPolicy, cancellationToken);
        }

        /// <inheritdoc/>
        public async Task<T> Execute<T>(
            Func<AsyncTransactionExecutor, Task<T>> func,
            RetryPolicy retryPolicy,
            CancellationToken cancellationToken = default)
        {
            return await this.sessionPool.Execute<T>(func, retryPolicy, cancellationToken);
        }

        /// <inheritdoc/>
        public async Task<IEnumerable<string>> ListTableNames(CancellationToken cancellationToken = default)
        {
            IAsyncResult result = await this.Execute(
                async txn => await txn.Execute(TableNameQuery), cancellationToken);

            return (await result.ToListAsync(cancellationToken)).Select(i => i.StringValue);
        }
    }
}
