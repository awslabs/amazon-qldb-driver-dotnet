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
    /// Represents a session to a specific ledger within QLDB, allowing for asynchronous execution of PartiQL statements
    /// and retrieval of the associated results, along with control over transactions for bundling multiple executions.
    /// </summary>
    internal class AsyncQldbSession : BaseQldbSession
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncQldbSession"/> class.
        /// </summary>
        ///
        /// <param name="session">The session object representing a communication channel with QLDB.</param>
        /// <param name="logger">The logger to be used by this.</param>
        internal AsyncQldbSession(Session session, ILogger logger, ISerializer serializer = null)
            : base(session, logger, serializer)
        {
        }

        /// <summary>
        /// Execute the Executor lambda asynchronously against QLDB and retrieve the result within a transaction.
        /// </summary>
        ///
        /// <param name="func">The Executor lambda representing the block of code to be executed within the transaction.
        /// This cannot have any side effects as it may be invoked multiple times, and the result cannot be trusted
        /// until the transaction is committed.</param>
        /// <param name="cancellationToken">A cancellation token that can be used by other objects or threads to receive
        /// notice of cancellation.</param>
        /// <typeparam name="T">The return type.</typeparam>
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
        /// <exception cref="AmazonServiceException">
        /// Thrown when there is an error executing against QLDB.
        /// </exception>
        internal async Task<T> Execute<T>(
            Func<AsyncTransactionExecutor, Task<T>> func,
            CancellationToken cancellationToken)
        {
            ValidationUtils.AssertNotNull(func, "func");

            AsyncTransaction transaction = null;
            string transactionId = QldbTransactionException.DefaultTransactionId;
            try
            {
                transaction = await this.StartTransaction(cancellationToken);
                transactionId = transaction.Id;
                T returnedValue = await func(new AsyncTransactionExecutor(transaction, serializer));
                if (returnedValue is IAsyncResult result)
                {
                    returnedValue = (T)(object)(await AsyncBufferedResult.BufferResultAsync(result));
                }

                await transaction.Commit();
                return returnedValue;
            }
            catch (TransactionAbortedException)
            {
                throw;
            }
            catch (InvalidSessionException ise)
            {
                if (IsTransactionExpiredException(ise))
                {
                    throw new QldbTransactionException(
                        transactionId,
                        await this.TryAbort(transaction, cancellationToken),
                        ise);
                }
                else
                {
                    throw new RetriableException(transactionId, false, ise);
                }
            }
            catch (OccConflictException occ)
            {
                throw new RetriableException(transactionId, true, occ);
            }
            catch (AmazonServiceException ase)
            {
                if (ase.StatusCode == HttpStatusCode.InternalServerError ||
                    ase.StatusCode == HttpStatusCode.ServiceUnavailable)
                {
                    throw new RetriableException(
                        transactionId,
                        await this.TryAbort(transaction, cancellationToken),
                        ase);
                }

                throw new QldbTransactionException(
                    transactionId,
                    await this.TryAbort(transaction, cancellationToken),
                    ase);
            }
            catch (Exception e)
            {
                throw new QldbTransactionException(
                    transactionId,
                    await this.TryAbort(transaction, cancellationToken),
                    e);
            }
            finally
            {
                transaction?.Dispose();
            }
        }

        /// <summary>
        /// Create a transaction object which allows for granular asynchronous control over when a transaction is
        /// aborted or committed.
        /// </summary>
        ///
        /// <param name="cancellationToken">
        /// A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// <returns>The newly created transaction object.</returns>
        internal virtual async Task<AsyncTransaction> StartTransaction(CancellationToken cancellationToken)
        {
            var startTransactionResult = await this.session.StartTransactionAsync(cancellationToken);
            return new AsyncTransaction(
                this.session,
                startTransactionResult.TransactionId,
                this.logger,
                cancellationToken);
        }

        /// <summary>
        /// Try to abort the transaction.
        /// </summary>
        ///
        /// <param name="transaction">
        /// The transaction to abort.
        /// </param>
        /// <param name="cancellationToken">
        /// A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// <returns>Whether the abort call has succeeded.</returns>
        private async Task<bool> TryAbort(AsyncTransaction transaction, CancellationToken cancellationToken)
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
            catch (Exception e)
            {
                this.logger.LogWarning("This session is invalid on ABORT: {}", e);
                return false;
            }

            return true;
        }
    }
}
