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
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// <para>The default implementation of Retry Handler.</para>
    ///
    /// <para>The driver retries in two scenarios: retrying inside a session, and retrying with another session. In the second case,
    /// it would require a <i>recover</i> action to reset the session into a working state.
    /// </summary>
    internal class RetryHandler : IRetryHandler
    {
        private readonly IEnumerable<Type> retryExceptions;
        private readonly IEnumerable<Type> exceptionsNeedRecover;
        private readonly ILogger logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="RetryHandler"/> class.
        /// </summary>
        /// <param name="limitedRetryExceptions">The exceptions that the handler would retry on.</param>
        /// <param name="unlimitedRetryExceptions">The exceptions that need to call the recover action on retry.</param>
        /// <param name="logger">The logger to record retries.</param>
        public RetryHandler(IEnumerable<Type> limitedRetryExceptions, IEnumerable<Type> unlimitedRetryExceptions, ILogger logger)
        {
            this.retryExceptions = limitedRetryExceptions;
            this.exceptionsNeedRecover = unlimitedRetryExceptions;
            this.logger = logger;
        }

        /// <inheritdoc/>
        public async Task<T> RetriableExecute<T>(Func<CancellationToken, Task<T>> func, RetryPolicy retryPolicy, Func<CancellationToken, Task> recoverAction, Func<int, CancellationToken, Task> retryAction, CancellationToken cancellationToken = default)
        {
            Exception last = null;
            int retryAttempt = 1;

            while (retryAttempt <= retryPolicy.MaxRetries + 1)
            {
                try
                {
                    return await func(cancellationToken);
                }
                catch (Exception ex)
                {
                    var uex = this.UnwrappedTransactionException(ex);

                    this.logger?.LogWarning(uex, "The driver retried on transaction '{}' {} times.", TryGetTransactionId(ex), retryAttempt);

                    if (!this.IsRetriable(uex))
                    {
                        throw uex;
                    }

                    last = !(uex is RetriableException) || uex.InnerException == null ? uex : uex.InnerException;

                    if (retryAction != null)
                    {
                        await retryAction(retryAttempt, cancellationToken);
                    }

                    if (this.NeedsRecover(uex))
                    {
                        await recoverAction(cancellationToken);
                    }
                    else
                    {
                        var backoffDelay = retryPolicy.BackoffStrategy.CalculateDelay(new RetryPolicyContext(retryAttempt, uex));
                        await Task.Delay(backoffDelay);
                        retryAttempt++;
                    }
                }
            }

            throw last;
        }

        public Task<T> RetriableExecute<T>(Func<Task<T>> func, RetryPolicy retryPolicy, Func<Task> recoverAction, Func<int, Task> retryAction, CancellationToken cancellationToken = default)
        {
            return this.RetriableExecute(ct => func(), retryPolicy, ct => recoverAction(), (arg, ct) => retryAction(arg), cancellationToken);
        }

        internal bool IsRetriable(Exception ex)
        {
            return FindException(this.retryExceptions, ex) || FindException(this.exceptionsNeedRecover, ex);
        }

        internal bool NeedsRecover(Exception ex)
        {
            return FindException(this.exceptionsNeedRecover, ex);
        }

        private static string TryGetTransactionId(Exception ex)
        {
            return ex is QldbTransactionException exception ? exception.TransactionId : string.Empty;
        }

        private static bool FindException(IEnumerable<Type> exceptions, Exception ex)
        {
            foreach (var i in exceptions)
            {
                if (IsSameOrSubclass(i, ex.GetType()))
                {
                    return true;
                }
            }

            return false;
        }

        private static bool IsSameOrSubclass(Type baseClass, Type childClass)
        {
            return baseClass == childClass || childClass.IsSubclassOf(baseClass);
        }

        private Exception UnwrappedTransactionException(Exception ex)
        {
            return ex is QldbTransactionException && ex.InnerException != null && this.IsRetriable(ex.InnerException) ? ex.InnerException : ex;
        }
    }
}
