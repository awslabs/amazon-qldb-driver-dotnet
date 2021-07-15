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
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.IonDotnet.Tree;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Implementation of a QLDB transaction. Any unexpected errors that occur when calling methods in this class should
    /// not be retried, as the state of the transaction is now ambiguous. When an OCC conflict occurs, the transaction
    /// is closed.
    /// </summary>
    internal class AsyncTransaction : BaseTransaction, IDisposable
    {
        private readonly CancellationToken cancellationToken;
        private readonly SemaphoreSlim hashLock;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncTransaction"/> class.
        /// </summary>
        ///
        /// <param name="session">The parent session that represents the communication channel to QLDB.</param>
        /// <param name="txnId">Transaction identifier.</param>
        /// <param name="logger">Logger to be used by this.</param>
        /// <param name="token">Propagates notification that operations should be canceled.</param>
        internal AsyncTransaction(Session session, string txnId, ILogger logger, CancellationToken token)
            : base(session, txnId, logger)
        {
            this.cancellationToken = token;
            this.hashLock = new SemaphoreSlim(1, 1);
        }

        public void Dispose()
        {
            this.hashLock.Dispose();
        }

        /// <summary>
        /// Abort the transaction asynchronously and roll back any changes.
        /// Any open <see cref="IAsyncResult"/> created by the transaction will be invalidated.
        /// </summary>
        ///
        /// <returns>A task representing the asynchronous abort operation.</returns>
        internal virtual async Task Abort()
        {
            await this.session.AbortTransactionAsync(this.cancellationToken);
        }

        /// <summary>
        /// Commit the transaction asynchronously.
        /// </summary>
        ///
        /// <exception cref="InvalidOperationException">
        /// Thrown when Hash returned from QLDB is not equal.
        /// </exception>
        /// <exception cref="OccConflictException">
        /// Thrown if an OCC conflict has been detected within the transaction.
        /// </exception>
        /// <exception cref="AmazonServiceException">
        /// Thrown when there is an error committing this transaction against QLDB.
        /// </exception
        ///
        /// <returns>A task representing the asynchronous commit operation.</returns>
        internal async Task Commit()
        {
            await this.hashLock.WaitAsync(this.cancellationToken);
            try
            {
                byte[] hashBytes = this.qldbHash.Hash;
                MemoryStream commitDigest = (await this.session.CommitTransactionAsync(
                        this.txnId,
                        new MemoryStream(hashBytes),
                        this.cancellationToken)).CommitDigest;
                if (!hashBytes.SequenceEqual(commitDigest.ToArray()))
                {
                    throw new InvalidOperationException(ExceptionMessages.TransactionDigestMismatch);
                }
            }
            finally
            {
                this.hashLock.Release();
            }
        }

        /// <summary>
        /// Execute the statement asynchronously using the specified parameters against QLDB and retrieve the result.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        ///
        /// <returns>Result from executed statement.</returns>
        ///
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        internal virtual async Task<IAsyncResult> Execute(string statement)
        {
            return await this.Execute(statement, new List<IIonValue>());
        }

        /// <summary>
        /// Execute the statement against QLDB and retrieve the result.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="parameters">Parameters to execute.</param>
        ///
        /// <returns>Result from executed statement.</returns>
        ///
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        internal virtual async Task<IAsyncResult> Execute(string statement, List<IIonValue> parameters)
        {
            ValidationUtils.AssertStringNotEmpty(statement, "statement");

            parameters ??= new List<IIonValue>();

            await this.hashLock.WaitAsync(this.cancellationToken);
            try
            {
                this.qldbHash = Dot(this.qldbHash, statement, parameters);
                ExecuteStatementResult executeStatementResult = await this.session.ExecuteStatementAsync(
                    this.txnId, statement, parameters, this.cancellationToken);
                return new AsyncResult(this.session, this.txnId, executeStatementResult, this.cancellationToken);
            }
            finally
            {
                this.hashLock.Release();
            }
        }

        internal virtual async Task<Generic.IAsyncResult<T>> Execute<T>(IQuery<T> query)
        {
            await this.hashLock.WaitAsync(this.cancellationToken);
            try
            {
                this.qldbHash = Dot(this.qldbHash, query.Statement, query.Parameters);
                ExecuteStatementResult executeStatementResult = await this.session.ExecuteStatementAsync(
                    this.txnId,
                    query.Statement,
                    query.Parameters,
                    this.cancellationToken);
                return new Generic.AsyncResult<T>(this.session, this.txnId, executeStatementResult, this.cancellationToken, query);
            }
            finally
            {
                this.hashLock.Release();
            }
        }

        /// <summary>
        /// Execute the statement using the specified parameters against QLDB and retrieve the result.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="parameters">Parameters to execute.</param>
        ///
        /// <returns>Result from executed statement.</returns>
        ///
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        internal virtual async Task<IAsyncResult> Execute(string statement, params IIonValue[] parameters)
        {
            return await this.Execute(statement, new List<IIonValue>(parameters));
        }
    }
}
