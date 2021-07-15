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
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Represents a session to a specific ledger within QLDB, allowing for execution of PartiQL statements and
    /// retrieval of the associated results, along with control over transactions for bundling multiple executions.
    /// </summary>
    internal class QldbSession : BaseQldbSession
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="QldbSession"/> class.
        /// </summary>
        ///
        /// <param name="session">The session object representing a communication channel with QLDB.</param>
        /// <param name="logger">The logger to be used by this.</param>
        /// <param name="serializer">The serializer to serialize and deserialize Ion data.</param>
        internal QldbSession(Session session, ILogger logger, ISerializer serializer = null)
            : base(session, logger, serializer)
        {
        }

        /// <summary>
        /// Execute the Executor lambda against QLDB and retrieve the result within a transaction.
        /// </summary>
        ///
        /// <param name="func">The Executor lambda representing the block of code to be executed within the transaction.
        /// This cannot have any side effects as it may be invoked multiple times, and the result cannot be trusted
        /// until the transaction is committed.</param>
        /// <typeparam name="T">The return type.</typeparam>
        ///
        /// <returns>The return value of executing the executor. Note that if you directly return a
        /// <see cref="IResult"/>, this will be automatically buffered in memory before the implicit commit to allow
        /// reading, as the commit will close any open results. Any other <see cref="IResult"/> instances created within
        /// the executor block will be invalidated, including if the return value is an object which nests said
        /// <see cref="IResult"/> instances within it.</returns>
        ///
        /// <exception cref="TransactionAbortedException">
        /// Thrown if the Executor lambda calls <see cref="TransactionExecutor.Abort"/>.
        /// </exception>
        /// <exception cref="AmazonServiceException">
        /// Thrown when there is an error executing against QLDB.
        /// </exception>
        internal T Execute<T>(Func<TransactionExecutor, T> func)
        {
            ValidationUtils.AssertNotNull(func, "func");

            Transaction transaction = null;
            string transactionId = QldbTransactionException.DefaultTransactionId;
            try
            {
                transaction = this.StartTransaction();
                transactionId = transaction.Id;
                T returnedValue = func(new TransactionExecutor(transaction, this.serializer));
                if (returnedValue is IResult result)
                {
                    returnedValue = (T)(object)BufferedResult.BufferResult(result);
                }

                transaction.Commit();
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
                    throw new QldbTransactionException(transactionId, this.TryAbort(transaction), ise);
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
                    throw new RetriableException(transactionId, this.TryAbort(transaction), ase);
                }

                throw new QldbTransactionException(transactionId, this.TryAbort(transaction), ase);
            }
            catch (Exception e)
            {
                throw new QldbTransactionException(transactionId, this.TryAbort(transaction), e);
            }
        }

        /// <summary>
        /// Create a transaction object which allows for granular control over when a transaction is aborted or
        /// committed.
        /// </summary>
        ///
        /// <returns>The newly created transaction object.</returns>
        internal Transaction StartTransaction()
        {
            var startTransactionResult = this.session.StartTransaction();
            return new Transaction(this.session, startTransactionResult.TransactionId, this.logger);
        }

        /// <summary>
        /// Try to abort the transaction.
        /// </summary>
        ///
        /// <param name="transaction">The transaction to abort.</param>
        ///
        /// <returns>Whether the abort call has succeeded.</returns>
        private bool TryAbort(Transaction transaction)
        {
            try
            {
                if (transaction != null)
                {
                    transaction.Abort();
                }
                else
                {
                    this.session.AbortTransaction();
                }
            }
            catch (AmazonServiceException ase)
            {
                this.logger.LogWarning("This session is invalid on ABORT: {}", ase);
                return false;
            }

            return true;
        }
    }
}
