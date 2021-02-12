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
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.QLDBSession.Model;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// <para>The asynchronous implementation of Retry Handler.</para>
    ///
    /// <para>The driver retries in two scenarios: retrying inside a session, and retrying with another session. In the
    /// second case, it would require a <i>recover</i> action to reset the session into a working state.
    /// </summary>
    internal class AsyncRetryHandler : BaseRetryHandler, IAsyncRetryHandler
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncRetryHandler"/> class.
        /// </summary>
        /// <param name="logger">The logger to record retries.</param>
        public AsyncRetryHandler(ILogger logger)
            : base(logger)
        {
        }

        /// <inheritdoc/>
        public async Task<T> RetriableExecute<T>(
            Func<CancellationToken, Task<T>> func,
            RetryPolicy retryPolicy,
            Func<CancellationToken, Task> newSessionAction,
            Func<CancellationToken, Task> nextSessionAction,
            Func<int, CancellationToken, Task> retryAction,
            CancellationToken cancellationToken = default)
        {
            int retryAttempt = 0;

            while (true)
            {
                try
                {
                    return await func(cancellationToken);
                }
                catch (QldbTransactionException ex)
                {
                    var iex = ex.InnerException != null ? ex.InnerException : ex;

                    if (!(ex is RetriableException))
                    {
                        throw iex;
                    }

                    if (retryAttempt < retryPolicy.MaxRetries && !IsTransactionExpiry(iex))
                    {
                        this.logger?.LogWarning(
                            iex,
                            "A recoverable exception has occurred. Attempting retry {}. Errored Transaction ID: {}.",
                            ++retryAttempt,
                            TryGetTransactionId(ex));

                        if (retryAction != null)
                        {
                            await retryAction(retryAttempt, cancellationToken);
                        }

                        var backoffDelay =
                            retryPolicy.BackoffStrategy.CalculateDelay(new RetryPolicyContext(retryAttempt, iex));
                        await Task.Delay(backoffDelay, cancellationToken);

                        if (!ex.IsSessionAlive)
                        {
                            if (iex is InvalidSessionException)
                            {
                                await newSessionAction(cancellationToken);
                            }
                            else
                            {
                                await nextSessionAction(cancellationToken);
                            }
                        }

                        continue;
                    }

                    throw iex;
                }
            }
        }
    }
}
