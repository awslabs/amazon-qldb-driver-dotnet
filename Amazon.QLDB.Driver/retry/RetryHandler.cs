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
        private readonly IEnumerable<Type> retryExceptions;
        private readonly IEnumerable<Type> exceptionsNeedRecover;
        private readonly ILogger logger;
        private readonly int recoverRetryLimit;

        /// <summary>
        /// Initializes a new instance of the <see cref="RetryHandler"/> class.
        /// </summary>
        /// <param name="limitedRetryExceptions">The exceptions that the handler would retry on.</param>
        /// <param name="recoverRetryExceptions">The exceptions that need to call the recover action on retry.</param>
        /// <param name="recoverRetryLimit">The limit of retries needing recover action.</param>
        /// <param name="logger">The logger to record retries.</param>
        public RetryHandler(IEnumerable<Type> limitedRetryExceptions, IEnumerable<Type> recoverRetryExceptions, int recoverRetryLimit, ILogger logger)
        {
            this.retryExceptions = limitedRetryExceptions;
            this.exceptionsNeedRecover = recoverRetryExceptions;
            this.recoverRetryLimit = recoverRetryLimit;
            this.logger = logger;
        }

        /// <inheritdoc/>
        public T RetriableExecute<T>(Func<T> func, RetryPolicy retryPolicy, Action recoverAction, Action<int> retryAction)
        {
            Exception last = null;
            int retryAttempt = 0;
            int recoverRetryAttempt = 0;

            while (true)
            {
                try
                {
                    return func.Invoke();
                }
                catch (Exception ex)
                {
                    var uex = this.UnwrappedTransactionException(ex);

                    last = !(uex is RetriableException) || uex.InnerException == null ? uex : uex.InnerException;

                    if (FindException(this.retryExceptions, uex) && retryAttempt < retryPolicy.MaxRetries)
                    {
                        this.logger?.LogWarning(uex, "A exception has occurred. Attempting retry {}. Errored Transaction ID: {}.",
                            ++retryAttempt, TryGetTransactionId(ex));

                        retryAction?.Invoke(retryAttempt);
                        Thread.Sleep(retryPolicy.BackoffStrategy.CalculateDelay(new RetryPolicyContext(retryAttempt, uex)));
                    }
                    else if (FindException(this.exceptionsNeedRecover, uex) && recoverRetryAttempt < this.recoverRetryLimit)
                    {
                        this.logger?.LogWarning(uex, "A recoverable exception has occurred. Attempting retry {}. Errored Transaction ID: {}.",
                            ++recoverRetryAttempt, TryGetTransactionId(ex));

                        retryAction?.Invoke(retryAttempt);
                        recoverAction();
                    }
                    else
                    {
                        throw last;
                    }
                }
            }

            throw last;
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
