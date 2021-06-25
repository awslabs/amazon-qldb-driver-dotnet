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
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Microsoft.Extensions.Logging;

    internal class QldbDriverBase<T> : IDisposable
        where T : BaseQldbSession
    {
        internal const string TableNameQuery =
                "SELECT VALUE name FROM information_schema.user_tables WHERE status = 'ACTIVE'";

        internal static readonly RetryPolicy DefaultRetryPolicy = RetryPolicy.Builder().Build();

        internal readonly string LedgerName;
        internal readonly IAmazonQLDBSession SessionClient;
        internal readonly ILogger Logger;
        internal readonly ISerializer Serializer;
        private readonly SemaphoreSlim poolPermits;
        private readonly BlockingCollection<T> sessionPool;
        private bool isClosed = false;

        internal QldbDriverBase(
            string ledgerName,
            IAmazonQLDBSession sessionClient,
            int maxConcurrentTransactions,
            ILogger logger,
            ISerializer serializer)
        {
            this.LedgerName = ledgerName;
            this.SessionClient = sessionClient;
            this.Logger = logger;
            this.poolPermits = new SemaphoreSlim(maxConcurrentTransactions, maxConcurrentTransactions);
            this.sessionPool = new BlockingCollection<T>(maxConcurrentTransactions);
            this.Serializer = serializer;
        }

        public void Dispose()
        {
            if (!this.isClosed)
            {
                this.isClosed = true;
                while (this.sessionPool.TryTake(out T session))
                {
                    session.End();
                }

                this.sessionPool.Dispose();
                this.SessionClient.Dispose();
                this.poolPermits.Dispose();
            }
        }

        internal void ReleaseSession(T session)
        {
            this.sessionPool.Add(session);
            this.Logger.LogDebug("Session returned to pool; pool size is now {}.", this.sessionPool.Count);
            this.poolPermits.Release();
        }

        internal void ThrowIfClosed()
        {
            if (this.isClosed)
            {
                this.Logger.LogError(ExceptionMessages.DriverClosed);
                throw new QldbDriverException(ExceptionMessages.DriverClosed);
            }
        }

        internal T GetSessionFromPool()
        {
            this.Logger.LogDebug(
                "Getting session. There are {} free sessions and {} available permits.",
                this.sessionPool.Count,
                this.sessionPool.BoundedCapacity - this.poolPermits.CurrentCount);

            if (this.poolPermits.Wait(0))
            {
                return this.sessionPool.TryTake(out T session) ? session : null;
            }
            else
            {
                this.Logger.LogError(ExceptionMessages.MaxConcurrentTransactionsExceeded);
                throw new QldbDriverException(ExceptionMessages.MaxConcurrentTransactionsExceeded);
            }
        }

        internal bool GetShouldReplaceDeadSessionOrThrowIfNoRetry(
            QldbTransactionException qte,
            T currentSession,
            int retryAttempt,
            RetryPolicy retryPolicy,
            Action<int> retryAction)
        {
            bool replaceDeadSession = this.GetIsSessionDeadAndThrowIfNoRetry(
                qte,
                currentSession,
                retryPolicy.MaxRetries,
                retryAttempt);
            try
            {
                retryAction?.Invoke(retryAttempt);
                Thread.Sleep(retryPolicy.BackoffStrategy.CalculateDelay(
                    new RetryPolicyContext(retryAttempt, qte.InnerException)));
            }
            catch (Exception)
            {
                // Safeguard against semaphore leak if parameter actions throw exceptions.
                if (replaceDeadSession)
                {
                    this.poolPermits.Release();
                }

                throw;
            }

            return replaceDeadSession;
        }

        internal async Task<bool> GetShouldReplaceDeadSessionOrThrowIfNoRetryAsync(
            QldbTransactionException qte,
            T currentSession,
            int retryAttempt,
            RetryPolicy retryPolicy,
            CancellationToken token)
        {
            bool replaceDeadSession = this.GetIsSessionDeadAndThrowIfNoRetry(
                qte,
                currentSession,
                retryPolicy.MaxRetries,
                retryAttempt);
            try
            {
                var backoffDelay = retryPolicy.BackoffStrategy.CalculateDelay(
                    new RetryPolicyContext(retryAttempt, qte.InnerException));
                await Task.Delay(backoffDelay, token);
            }
            catch (Exception)
            {
                // Safeguard against semaphore leak if parameter actions throw exceptions.
                if (replaceDeadSession)
                {
                    this.poolPermits.Release();
                }

                throw;
            }

            return replaceDeadSession;
        }

        private bool GetIsSessionDeadAndThrowIfNoRetry(
            QldbTransactionException qte,
            T currentSession,
            int maxRetries,
            int retryAttempt)
        {
            if (qte is RetriableException)
            {
                // Always retry on the first attempt if failure was caused by a stale session in the pool.
                if (qte.InnerException is InvalidSessionException && retryAttempt == 1)
                {
                    this.Logger.LogDebug("Initial session received from pool invalid. Retrying...");
                    return true;
                }

                // Normal retry logic.
                if (retryAttempt > maxRetries)
                {
                    if (qte.IsSessionAlive)
                    {
                        this.ReleaseSession(currentSession);
                    }
                    else
                    {
                        this.poolPermits.Release();
                    }

                    throw qte.InnerException;
                }

                this.Logger.LogInformation("A recoverable error has occurred. Attempting retry #{}.", retryAttempt);
                this.Logger.LogDebug(
                    "Errored Transaction ID: {}. Error cause: {}",
                    qte.TransactionId,
                    qte.InnerException.ToString());
                if (qte.IsSessionAlive)
                {
                    this.Logger.LogDebug("Retrying with a different session...");
                    this.ReleaseSession(currentSession);
                }
                else
                {
                    this.Logger.LogDebug("Replacing invalid session...");
                }

                return !qte.IsSessionAlive;
            }
            else
            {
                if (qte.IsSessionAlive)
                {
                    this.ReleaseSession(currentSession);
                }
                else
                {
                    this.poolPermits.Release();
                }

                throw qte.InnerException;
            }
        }
    }
}
