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
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;

    internal class AsyncRetryHandler : IAsyncRetryHandler
    {
        private readonly ILogger logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncRetryHandler"/> class.
        /// </summary>
        /// <param name="logger">The logger to record retries.</param>
        public AsyncRetryHandler(ILogger logger)
        {
            this.logger = logger;
        }

        public Task<T> RetriableExecute<T>(Func<T> func, RetryPolicy retryPolicy, Action newSessionAction, Action nextSessionAction, Action<int> retryAction)
        {
            throw new NotImplementedException();
        }
    }
}
