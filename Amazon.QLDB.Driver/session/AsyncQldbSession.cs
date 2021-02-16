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
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// <para>Represents a session to a specific ledger within QLDB, allowing for asynchronous execution of PartiQL statements and
    /// retrieval of the associated results, along with control over transactions for bundling multiple executions.</para>
    ///
    /// <para>The execute methods provided will automatically retry themselves in the case that an unexpected recoverable error
    /// occurs, including OCC conflicts, by starting a brand new transaction and re-executing the statement within the new
    /// transaction.</para>
    ///
    /// </summary>
    internal class AsyncQldbSession : BaseQldbSession
    {
        private readonly Action<AsyncQldbSession> releaseSession;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncQldbSession"/> class.
        /// </summary>
        ///
        /// <param name="session">The session object representing a communication channel with QLDB.</param>
        /// <param name="releaseSession">The delegate method to release the session.</param>
        /// <param name="logger">The logger to be used by this.</param>
        internal AsyncQldbSession(Session session, Action<AsyncQldbSession> releaseSession, ILogger logger)
            : base(session, logger)
        {
            this.releaseSession = releaseSession;
        }

        /// <summary>
        /// Release the session which still can be used by another transaction.
        /// </summary>
        internal override void Release()
        {
            this.releaseSession(this);
        }

        /// <summary>
        /// Execute the Executor lambda asynchronously against QLDB and retrieve the result within a transaction.
        /// </summary>
        ///
        /// <param name="func">The Executor lambda representing the block of code to be executed within the transaction.
        /// This cannot have any side effects as it may be invoked multiple times, and the result cannot be trusted
        /// until the transaction is committed.</param>
        /// <typeparam name="T">The return type.</typeparam>
        /// <param name="cancellationToken">
        /// A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// <returns>The return value of executing the executor. Note that if you directly return a
        /// <see cref="IAsyncResult"/>, this will be automatically buffered in memory before the implicit commit to
        /// allow reading, as the commit will close any open results. Any other <see cref="IAsyncResult"/> instances
        /// created within the executor block will be invalidated, including if the return value is an object which
        /// nests said <see cref="IAsyncResult"/> instances within it.
        /// </returns>
        ///
        /// <exception cref="TransactionAbortedException">
        /// Thrown if the Executor lambda calls <see cref="AsyncTransactionExecutor.Abort"/>.
        /// </exception>
        /// <exception cref="QldbDriverException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        internal async Task<T> Execute<T>(
            Func<AsyncTransactionExecutor, Task<T>> func, CancellationToken cancellationToken = default)
        {
            ValidationUtils.AssertNotNull(func, "func");

            AsyncTransaction transaction = null;
            string transactionId = "None";
            try
            {
                transaction = await this.StartTransaction(cancellationToken);
                transactionId = transaction.Id;
                T returnedValue = await func(new AsyncTransactionExecutor(transaction));
                if (returnedValue is IAsyncResult result)
                {
                    returnedValue = (T)(object)(await AsyncBufferedResult.BufferResultAsync(result));
                }

                await transaction.Commit();
                return returnedValue;
            }
            catch (InvalidSessionException ise)
            {
                this.isAlive = false;
                throw new RetriableException(transactionId, false, ise);
            }
            catch (OccConflictException occ)
            {
                throw new RetriableException(transactionId, occ);
            }
            catch (AmazonServiceException ase)
            {
                if (ase.StatusCode == HttpStatusCode.InternalServerError ||
                    ase.StatusCode == HttpStatusCode.ServiceUnavailable)
                {
                    throw new RetriableException(
                        transactionId, await this.TryAbort(transaction, cancellationToken), ase);
                }

                throw new QldbTransactionException(
                    transactionId, await this.TryAbort(transaction, cancellationToken), ase);
            }
            catch (QldbTransactionException te)
            {
                throw te;
            }
            catch (Exception e)
            {
                throw new QldbTransactionException(
                    transactionId, await this.TryAbort(transaction, cancellationToken), e);
            }
        }

        /// <summary>
        /// Create a transaction object which allows for granular asynchronous control over when a transaction is aborted or committed.
        /// </summary>
        ///
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// <returns>The newly created transaction object.</returns>
        internal virtual async Task<AsyncTransaction> StartTransaction(CancellationToken cancellationToken = default)
        {
            try
            {
                var startTransactionResult = await this.session.StartTransactionAsync(cancellationToken);
                return new AsyncTransaction(
                    this.session,
                    startTransactionResult.TransactionId,
                    this.logger);
            }
            catch (BadRequestException e)
            {
                throw new QldbTransactionException(
                    ExceptionMessages.TransactionAlreadyOpened,
                    string.Empty,
                    await this.TryAbort(null, cancellationToken),
                    e);
            }
        }

        /// <summary>
        /// Try to abort the transaction.
        /// </summary>
        ///
        /// <param name="transaction">The transaction to abort.</param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// <returns>Whether the abort call has succeeded.</returns>
        /// <exception cref="AmazonServiceException">If there is an error communicating with QLDB.</exception>
        private async Task<bool> TryAbort(AsyncTransaction transaction, CancellationToken cancellationToken = default)
        {
            try
            {
                if (transaction != null)
                {
                    await transaction.Abort();
                }
                else
                {
                    await this.session.AbortTransactionAsync(cancellationToken);
                }
            }
            catch (AmazonServiceException ase)
            {
                this.logger.LogWarning("This session is invalid on ABORT: {}", ase);
                this.isAlive = false;
                return false;
            }

            return true;
        }
    }
}
