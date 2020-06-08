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

    internal class RetryHandler : IRetryHandler
    {
        private readonly int retryLimit;
        private readonly IEnumerable<Type> retryExceptions;
        private readonly IEnumerable<Type> exceptionsNeedRecover;

        public RetryHandler(int retryLimit, IEnumerable<Type> retryExceptions, IEnumerable<Type> exceptionsNeedRecover)
        {
            this.retryLimit = retryLimit;
            this.retryExceptions = retryExceptions;
            this.exceptionsNeedRecover = exceptionsNeedRecover;
        }

        public T RetriableExecute<T>(Func<T> func, Action<int> retryAction, Action recoverAction)
        {
            Exception last = null;
            for (int i = 0; i < this.retryLimit + 1; i++)
            {
                try
                {
                    return func.Invoke();
                }
                catch (Exception ex)
                {
                    if (!this.IsRetriable(ex))
                    {
                        throw ex;
                    }

                    last = ex;

                    if (this.NeedsRecover(ex))
                    {
                        recoverAction.Invoke();
                    }

                    if (retryAction != null)
                    {
                        retryAction.Invoke(i);
                    }

                    SleepOnRetry(i);
                }
            }

            throw last;
        }

        internal bool IsRetriable(Exception ex)
        {
            return FindException(this.retryExceptions, ex);
        }

        internal bool NeedsRecover(Exception ex)
        {
            return FindException(this.exceptionsNeedRecover, ex);
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

        /// <summary>
        /// Sleep for an exponentially increasing amount relative to executionAttempt.
        /// </summary>
        ///
        /// <param name="executionAttempt">The number of execution attempts.</param>
        private static void SleepOnRetry(int executionAttempt)
        {
            const int SleepBaseMilliseconds = 10;
            const int SleepCapMilliseconds = 5000;
            var rng = new Random();
            var jitterRand = rng.NextDouble();
            var exponentialBackoff = Math.Min(SleepCapMilliseconds, Math.Pow(SleepBaseMilliseconds, executionAttempt));
            Thread.Sleep(Convert.ToInt32(jitterRand * (exponentialBackoff + 1)));
        }

        private static bool IsSameOrSubclass(Type baseClass, Type childClass)
        {
            return baseClass == childClass || childClass.IsSubclassOf(baseClass);
        }
    }
}
