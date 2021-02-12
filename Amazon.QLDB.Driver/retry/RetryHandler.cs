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
    using Amazon.QLDBSession.Model;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// <para>The default implementation of Retry Handler.</para>
    ///
    /// <para>The driver retries in two scenarios: retrying inside a session, and retrying with another session. In the
    /// second case, it would require a <i>recover</i> action to reset the session into a working state.
    /// </summary>
    internal class RetryHandler : BaseRetryHandler, IRetryHandler
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="RetryHandler"/> class.
        /// </summary>
        ///
        /// <param name="logger">The logger to record retries.</param>
        public RetryHandler(ILogger logger)
            : base(logger)
        {
        }

        /// <inheritdoc/>
        public T RetriableExecute<T>(
            Func<T> func,
            RetryPolicy retryPolicy,
            Action newSessionAction,
            Action nextSessionAction,
            Action<int> retryAction)
        {
            int retryAttempt = 0;

            while (true)
            {
                try
                {
                    return func();
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

                        retryAction?.Invoke(retryAttempt);

                        Thread.Sleep(
                            retryPolicy.BackoffStrategy.CalculateDelay(new RetryPolicyContext(retryAttempt, iex)));

                        if (!ex.IsSessionAlive)
                        {
                            if (iex is InvalidSessionException)
                            {
                                newSessionAction();
                            }
                            else
                            {
                                nextSessionAction();
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
