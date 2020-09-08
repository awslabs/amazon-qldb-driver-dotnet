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
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// <para>Represents a session to a specific ledger within QLDB, allowing for execution of PartiQL statements and
    /// retrieval of the associated results, along with control over transactions for bundling multiple executions.</para>
    ///
    /// <para>The execute methods provided will automatically retry themselves in the case that an unexpected recoverable error
    /// occurs, including OCC conflicts, by starting a brand new transaction and re-executing the statement within the new
    /// transaction.</para>
    ///
    /// <para>There are three methods of execution, ranging from simple to complex; the first two are recommended for their inbuilt
    /// error handling:
    /// <list type="bullet">
    /// <item><description>Execute(string, Action), Execute(string, Action, List), and Execute(string, Action, params IIonValue[])
    /// allow for a single statement to be executed within a transaction where the transaction is implicitly created
    /// and committed, and any recoverable errors are transparently handled. Each parameter besides the statement string
    /// have overloaded method variants where they are not necessary.</description></item>
    /// <item><description>Execute(Action, Action) and Execute(Func, Action) allow for more complex execution sequences where
    /// more than one execution can occur, as well as other method calls. The transaction is implicitly created and committed, and any
    /// recoverable errors are transparently handled. The second Action parameter has overloaded variants where it is not
    /// necessary.</description></item>
    /// <item><description><see cref="StartTransaction"/> allows for full control over when the transaction is committed and leaves the
    /// responsibility of OCC conflict handling up to the user. Transaction methods cannot be automatically retried, as
    /// the state of the transaction is ambiguous in the case of an unexpected error.</description></item>
    /// </list>
    /// </para>
    /// </summary>
    internal class QldbSession
    {
        private readonly ILogger logger;
        private readonly Action<QldbSession> releaseSession;
        private Session session;
        private bool isAlive;

        /// <summary>
        /// Initializes a new instance of the <see cref="QldbSession"/> class.
        /// </summary>
        ///
        /// <param name="session">The session object representing a communication channel with QLDB.</param>
        /// <param name="releaseSession">The delegate method to release the session.</param>
        /// <param name="logger">The logger to be used by this.</param>
        internal QldbSession(Session session, Action<QldbSession> releaseSession, ILogger logger)
        {
            this.session = session;
            this.releaseSession = releaseSession;
            this.logger = logger;
            this.isAlive = true;
        }

        public bool IsAlive()
        {
            return this.isAlive;
        }

        /// <summary>
        /// Close the internal session object.
        /// </summary>
        ///
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        public async Task Close(CancellationToken cancellationToken = default)
        {
            await this.session.End(cancellationToken);
        }

        /// <summary>
        /// Release the session which still can be used by another transaction.
        /// </summary>
        public void Release()
        {
            this.releaseSession(this);
        }

        /// <summary>
        /// Execute the Executor lambda against QLDB and retrieve the result within a transaction.
        /// </summary>
        ///
        /// <param name="func">The Executor lambda representing the block of code to be executed within the transaction. This cannot have any
        /// side effects as it may be invoked multiple times, and the result cannot be trusted until the
        /// transaction is committed.</param>
        /// <typeparam name="T">The return type.</typeparam>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// <returns>The return value of executing the executor. Note that if you directly return a <see cref="IResult"/>, this will
        /// be automatically buffered in memory before the implicit commit to allow reading, as the commit will close
        /// any open results. Any other <see cref="IResult"/> instances created within the executor block will be
        /// invalidated, including if the return value is an object which nests said <see cref="IResult"/> instances within it.
        /// </returns>
        ///
        /// <exception cref="TransactionAbortedException">Thrown if the Executor lambda calls <see cref="TransactionExecutor.Abort"/>.</exception>
        /// <exception cref="QldbDriverException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        public Task<T> Execute<T>(Func<TransactionExecutor, Task<T>> func, CancellationToken cancellationToken = default)
        {
            return this.Execute((transactionExecutor, token) => func(transactionExecutor), cancellationToken);
        }

        /// <summary>
        /// Execute the Executor lambda against QLDB and retrieve the result within a transaction.
        /// </summary>
        ///
        /// <param name="func">The Executor lambda representing the block of code to be executed within the transaction. This cannot have any
        /// side effects as it may be invoked multiple times, and the result cannot be trusted until the
        /// transaction is committed. The operation can be cancelled.</param>
        /// <typeparam name="T">The return type.</typeparam>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// <returns>The return value of executing the executor. Note that if you directly return a <see cref="IResult"/>, this will
        /// be automatically buffered in memory before the implicit commit to allow reading, as the commit will close
        /// any open results. Any other <see cref="IResult"/> instances created within the executor block will be
        /// invalidated, including if the return value is an object which nests said <see cref="IResult"/> instances within it.
        /// </returns>
        ///
        /// <exception cref="TransactionAbortedException">Thrown if the Executor lambda calls <see cref="TransactionExecutor.Abort"/>.</exception>
        /// <exception cref="QldbDriverException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        public async Task<T> Execute<T>(Func<TransactionExecutor, CancellationToken, Task<T>> func, CancellationToken cancellationToken = default)
        {
            ValidationUtils.AssertNotNull(func, "func");

            ITransaction transaction = null;
            try
            {
                transaction = await this.StartTransaction(cancellationToken);
                T returnedValue = await func(new TransactionExecutor(transaction), cancellationToken);
                if (returnedValue is IResult)
                {
                    returnedValue = (T)(object)await BufferedResult.BufferResult((IResult)returnedValue);
                }

                await transaction.Commit(cancellationToken);
                return returnedValue;
            }
            catch (InvalidSessionException ise)
            {
                this.isAlive = false;
                throw new RetriableException(transaction.Id, false, ise);
            }
            catch (OccConflictException occ)
            {
                throw new RetriableException(transaction.Id, occ);
            }
            catch (AmazonServiceException ase)
            {
                if (ase.StatusCode == HttpStatusCode.InternalServerError ||
                    ase.StatusCode == HttpStatusCode.ServiceUnavailable)
                {
                    throw new RetriableException(transaction.Id, await this.TryAbort(transaction, cancellationToken), ase);
                }

                throw new QldbTransactionException(transaction.Id, await this.TryAbort(transaction, cancellationToken), ase);
            }
            catch (QldbTransactionException te)
            {
                throw te;
            }
            catch (Exception e)
            {
                throw new QldbTransactionException(transaction == null ? null : transaction.Id, await this.TryAbort(transaction, cancellationToken), e);
            }
        }

        /// <summary>
        /// Create a transaction object which allows for granular control over when a transaction is aborted or committed.
        /// </summary>
        ///
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// <returns>The newly created transaction object.</returns>
        public virtual async Task<ITransaction> StartTransaction(CancellationToken cancellationToken = default)
        {
            try
            {
                var startTransactionResult = await this.session.StartTransaction(cancellationToken);
                return new Transaction(this.session, startTransactionResult.TransactionId, this.logger);
            }
            catch (BadRequestException e)
            {
                throw new QldbTransactionException(ExceptionMessages.TransactionAlreadyOpened, string.Empty, await this.TryAbort(null, cancellationToken), e);
            }
        }

        /// <summary>
        /// Retrieve the ID of this session..
        /// </summary>
        ///
        /// <returns>The ID of this session.</returns>
        internal string GetSessionId()
        {
            return this.session.SessionId;
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
        private async Task<bool> TryAbort(ITransaction transaction, CancellationToken cancellationToken = default)
        {
            try
            {
                if (transaction != null)
                {
                    await transaction.Abort(cancellationToken);
                }
                else
                {
                    await this.session.AbortTransaction(cancellationToken);
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
