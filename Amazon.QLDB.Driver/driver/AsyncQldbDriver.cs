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
    using System.Threading.Tasks;

    public class AsyncQldbDriver : BaseQldbDriver, IAsyncQldbDriver
    {
        private readonly AsyncSessionPool sessionPool;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncQldbDriver"/> class.
        /// </summary>
        ///
        /// <param name="sessionPool">The ledger to create sessions to.</param>
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

        public Task Execute(Action<AsyncTransactionExecutor> action)
        {
            throw new NotImplementedException();
        }

        public Task Execute(Action<AsyncTransactionExecutor> action, RetryPolicy retryPolicy)
        {
            throw new NotImplementedException();
        }

        public Task<T> Execute<T>(Func<AsyncTransactionExecutor, T> func)
        {
            throw new NotImplementedException();
        }

        public Task<T> Execute<T>(Func<AsyncTransactionExecutor, T> func, RetryPolicy retryPolicy)
        {
            throw new NotImplementedException();
        }

        internal Task<T> Execute<T>(Func<AsyncTransactionExecutor, T> func, RetryPolicy retryPolicy, Action<int> retryAction)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<string> ListTableNames()
        {
            throw new NotImplementedException();
        }
    }
}
