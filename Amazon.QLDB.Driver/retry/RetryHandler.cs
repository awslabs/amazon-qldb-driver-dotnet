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
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.QLDBSession.Model;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// <para>The default implementation of Retry Handler.</para>
    ///
    /// <para>The driver retries in two scenarios: retrying inside a session, and retrying with another session. In the second case,
    /// it would require a <i>recover</i> action to reset the session into a working state.
    /// </summary>
    internal class RetryHandler : IRetryHandler
    {
        private readonly ILogger logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="RetryHandler"/> class.
        /// </summary>
        /// <param name="logger">The logger to record retries.</param>
        public RetryHandler(ILogger logger)
        {
            this.logger = logger;
        }

        /// <inheritdoc/>
        public async Task<T> RetriableExecute<T>(Func<CancellationToken, Task<T>> func, RetryPolicy retryPolicy, Func<CancellationToken, Task> newSessionAction, Func<CancellationToken, Task> nextSessionAction, Func<int, CancellationToken, Task> retryAction, CancellationToken cancellationToken = default)
        {
            Exception last = null;
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

                    last = iex;

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

                        var backoffDelay = retryPolicy.BackoffStrategy.CalculateDelay(new RetryPolicyContext(retryAttempt, iex));
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

                    throw last;
                }
            }

            throw last;
        }

        public Task<T> RetriableExecute<T>(Func<Task<T>> func, RetryPolicy retryPolicy, Func<Task> newSession, Func<Task> nextSession, Func<int, Task> retryAction, CancellationToken cancellationToken = default)
        {
            return this.RetriableExecute(ct => func(), retryPolicy, ct => newSession(), ct => nextSession(), (arg, ct) => retryAction(arg), cancellationToken);
        }

        internal static bool IsTransactionExpiry(Exception ex)
        {
            return ex is InvalidSessionException
                && Regex.Match(ex.Message, @"Transaction\s.*\shas\sexpired").Success;
        }

        private static string TryGetTransactionId(Exception ex)
        {
            return ex is QldbTransactionException exception ? exception.TransactionId : string.Empty;
        }
    }
}
