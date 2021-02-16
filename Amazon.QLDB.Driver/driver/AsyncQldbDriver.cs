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

    public class AsyncQldbDriver : BaseQldbDriver, IAsyncQldbDriver
    {
        private readonly AsyncSessionPool sessionPool;

        internal AsyncQldbDriver(AsyncSessionPool sessionPool)
        {
            this.sessionPool = sessionPool;
        }

        public static AsyncQldbDriverBuilder Builder()
        {
            return new AsyncQldbDriverBuilder();
        }

        public void Dispose()
        {
            this.sessionPool.Dispose();
        }

        public async Task Execute(
            Func<AsyncTransactionExecutor, Task> action,
            CancellationToken cancellationToken = default)
        {
            await this.Execute(action, BaseQldbDriver.DefaultRetryPolicy, cancellationToken);
        }

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

        public async Task<T> Execute<T>(
            Func<AsyncTransactionExecutor, Task<T>> func, CancellationToken cancellationToken = default)
        {
            return await this.Execute<T>(func, BaseQldbDriver.DefaultRetryPolicy, cancellationToken);
        }

        public async Task<T> Execute<T>(
            Func<AsyncTransactionExecutor, Task<T>> func,
            RetryPolicy retryPolicy,
            CancellationToken cancellationToken = default)
        {
            return await this.sessionPool.Execute<T>(func, retryPolicy, cancellationToken);
        }

        public async Task<IEnumerable<string>> ListTableNamesAsync(CancellationToken cancellationToken = default)
        {
            IAsyncResult result = await this.Execute<IAsyncResult>(
                async txn =>
                {
                    return await txn.Execute(TableNameQuery);
                }, cancellationToken);

            return (await result.ToListAsync()).Select(i => i.StringValue);
        }
    }
}
