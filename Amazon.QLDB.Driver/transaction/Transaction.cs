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
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.IonDotnet.Tree;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Implementation of a QLDB transaction which also tracks child Results for the purposes of managing their lifecycle.
    /// Any unexpected errors that occur when calling methods in this class should not be retried, as the state of the
    /// transaction is now ambiguous. When an OCC conflict occurs, the transaction is closed.
    ///
    /// Child Result objects will be closed when the transaction is aborted or committed.
    /// </summary>
    internal class Transaction : ITransaction
    {
        private readonly Session session;
        private readonly string txnId;
        private readonly ILogger logger;
        private QldbHash qldbHash;
        private bool isClosed = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="Transaction"/> class.
        /// </summary>
        ///
        /// <param name="session">The parent session that represents the communication channel to QLDB.</param>
        /// <param name="txnId">Transaction identifier.</param>
        /// <param name="logger">Logger to be used by this.</param>
        internal Transaction(Session session, string txnId, ILogger logger)
        {
            this.session = session;
            this.txnId = txnId;
            this.logger = logger;
            this.qldbHash = QldbHash.ToQldbHash(txnId);
        }

        /// <summary>
        /// Gets the transaction ID.
        /// </summary>
        public string Id => this.txnId;

        /// <summary>
        /// Abort the transaction and roll back any changes. No-op if closed.
        /// Any open <see cref="IResult"/> created by the transaction will be invalidated.
        /// </summary>
        ///
        /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
        ///
        /// <returns>A task representing the asynchronous abort operation.</returns>
        public async Task Abort(CancellationToken cancellationToken = default)
        {
            if (!this.isClosed)
            {
                this.isClosed = true;
                await this.session.AbortTransaction(cancellationToken);
            }
        }

        /// <summary>
        /// Commit the transaction. Any open <see cref="IResult"/> created by the transaction will be invalidated.
        /// </summary>
        ///
        /// <param name="cancellationToken">Propagates notification that operations should be canceled.</param>
        ///
        /// <exception cref="InvalidOperationException">Thrown when Hash returned from QLDB is not equal.</exception>
        /// <exception cref="OccConflictException">Thrown if an OCC conflict has been detected within the transaction.</exception>
        /// <exception cref="AmazonServiceException">Thrown when there is an error committing this transaction against QLDB.</exception>
        /// <exception cref="QldbDriverException">Thrown when this transaction has been disposed.</exception>
        /// <returns>A task representing the asynchronous commit operation.</returns>
        public async Task Commit(CancellationToken cancellationToken = default)
        {
            try
            {
                byte[] hashBytes = this.qldbHash.Hash;
                MemoryStream commitDigest = (await this.session.CommitTransaction(this.txnId, new MemoryStream(hashBytes), cancellationToken))
                    .CommitDigest;
                if (!hashBytes.SequenceEqual(commitDigest.ToArray()))
                {
                    throw new InvalidOperationException(ExceptionMessages.TransactionDigestMismatch);
                }
            }
            catch (OccConflictException oce)
            {
                throw oce;
            }
            catch (InvalidSessionException ise)
            {
                throw ise;
            }
            catch (AmazonServiceException ase)
            {
                await this.DisposeAsync();
                throw ase;
            }
            finally
            {
                this.isClosed = true;
            }
        }

        /// <summary>
        /// Abort the transaction and close it. No-op if already closed.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            try
            {
                await this.Abort();
            }
            catch (AmazonServiceException ase)
            {
                this.logger.LogWarning("Ignored AmazonServiceException aborting transaction when calling dispose: {}", ase);
            }
        }

        /// <summary>
        /// Execute the statement using the specified parameters against QLDB and retrieve the result.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// 
        /// <returns>Result from executed statement.</returns>
        ///
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        /// <exception cref="QldbDriverException">Thrown when this transaction has been disposed.</exception>
        public async Task<IResult> Execute(string statement, CancellationToken cancellationToken = default)
        {
            return await this.Execute(statement, new List<IIonValue>(), cancellationToken);
        }

        /// <summary>
        /// Execute the statement against QLDB and retrieve the result.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="parameters">Parameters to execute.</param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// 
        /// <returns>Result from executed statement.</returns>
        ///
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        /// <exception cref="QldbDriverException">Thrown when this transaction has been disposed.</exception>
        public async Task<IResult> Execute(string statement, List<IIonValue> parameters, CancellationToken cancellationToken = default)
        {
            ValidationUtils.AssertStringNotEmpty(statement, "statement");

            if (parameters == null)
            {
                parameters = new List<IIonValue>();
            }

            this.qldbHash = Dot(this.qldbHash, statement, parameters);
            ExecuteStatementResult executeStatementResult = await this.session.ExecuteStatement(
                this.txnId, statement, parameters, cancellationToken);
            return new Result(this.session, this.txnId, executeStatementResult.FirstPage);
        }

        /// <summary>
        /// Execute the statement using the specified parameters against QLDB and retrieve the result.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// <param name="parameters">Parameters to execute.</param>
        ///
        /// <returns>Result from executed statement.</returns>
        ///
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        /// <exception cref="QldbDriverException">Thrown when this transaction has been disposed.</exception>
        public Task<IResult> Execute(string statement, CancellationToken cancellationToken = default, params IIonValue[] parameters)
        {
            return this.Execute(statement, new List<IIonValue>(parameters), cancellationToken);
        }

        /// <summary>
        /// Calculate the QLDB hash from statement and parameters.
        /// </summary>
        ///
        /// <param name="seed">QLDB Hash.</param>
        /// <param name="statement">PartiQL statement.</param>
        /// <param name="parameters">Parameters to execute.</param>
        ///
        /// <returns>QLDB hash.</returns>
        internal static QldbHash Dot(QldbHash seed, string statement, List<IIonValue> parameters)
        {
            QldbHash statementHash = QldbHash.ToQldbHash(statement);
            foreach (var ionValue in parameters)
            {
                statementHash = statementHash.Dot(QldbHash.ToQldbHash(ionValue));
            }

            return seed.Dot(statementHash);
        }
    }
}
