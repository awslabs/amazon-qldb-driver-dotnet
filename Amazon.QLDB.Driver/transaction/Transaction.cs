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
    using Amazon.IonDotnet.Tree;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Implementation of a QLDB transaction. Any unexpected errors that occur when calling methods in this class should
    /// not be retried, as the state of the transaction is now ambiguous. When an OCC conflict occurs, the transaction
    /// is closed.
    /// </summary>
    internal class Transaction : BaseTransaction
    {
        private readonly object hashLock = new object();

        /// <summary>
        /// Initializes a new instance of the <see cref="Transaction"/> class.
        /// </summary>
        ///
        /// <param name="session">The parent session that represents the communication channel to QLDB.</param>
        /// <param name="txnId">Transaction identifier.</param>
        /// <param name="logger">Logger to be used by this.</param>
        internal Transaction(Session session, string txnId, ILogger logger)
            : base(session, txnId, logger)
        {
        }

        /// <summary>
        /// Abort the transaction and roll back any changes.
        /// </summary>
        internal virtual void Abort()
        {
            this.session.AbortTransaction();
        }

        /// <summary>
        /// Commit the transaction.
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
        /// </exception>
        internal void Commit()
        {
            // Prevent race condition between Commit and Executes, making them synchronous to ensure hash validity.
            lock (this.hashLock)
            {
                byte[] hashBytes = this.qldbHash.Hash;
                MemoryStream commitDigest = this.session.CommitTransaction(this.txnId, new MemoryStream(hashBytes))
                    .CommitDigest;
                if (!hashBytes.SequenceEqual(commitDigest.ToArray()))
                {
                    throw new InvalidOperationException(ExceptionMessages.TransactionDigestMismatch);
                }
            }
        }

        /// <summary>
        /// Execute the statement using the specified parameters against QLDB and retrieve the result.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        ///
        /// <returns>Result from executed statement.</returns>
        ///
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        internal virtual IResult Execute(string statement)
        {
            return this.Execute(statement, new List<IIonValue>());
        }

        internal Amazon.QLDB.Driver.Generic.IResult<T> Execute<T>(IQuery<T> query)
        {
            lock (this.hashLock)
            {
                this.qldbHash = Dot(this.qldbHash, query.Statement, query.Parameters);
                ExecuteStatementResult executeStatementResult = this.session.ExecuteStatement(
                    this.txnId, 
                    query.Statement, 
                    query.Parameters);
                return new Amazon.QLDB.Driver.Generic.Result<T>(this.session, this.txnId, executeStatementResult, query);
            }
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
        internal virtual IResult Execute(string statement, List<IIonValue> parameters)
        {
            ValidationUtils.AssertStringNotEmpty(statement, "statement");

            parameters ??= new List<IIonValue>();

            // Prevent race condition between Commit and Executes, making them synchronous to ensure hash validity.
            lock (this.hashLock)
            {
                this.qldbHash = Dot(this.qldbHash, statement, parameters);
                ExecuteStatementResult executeStatementResult = this.session.ExecuteStatement(
                    this.txnId, statement, parameters);
                return new Result(this.session, this.txnId, executeStatementResult);
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
        internal virtual IResult Execute(string statement, params IIonValue[] parameters)
        {
            return this.Execute(statement, new List<IIonValue>(parameters));
        }
    }
}
