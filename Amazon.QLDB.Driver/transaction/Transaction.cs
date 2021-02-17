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
    /// Implementation of a QLDB transaction which also tracks child Results for the purposes of managing their lifecycle.
    /// Any unexpected errors that occur when calling methods in this class should not be retried, as the state of the
    /// transaction is now ambiguous. When an OCC conflict occurs, the transaction is closed.
    ///
    /// Child Result objects will be closed when the transaction is aborted or committed.
    /// </summary>
    internal class Transaction : BaseTransaction, ITransaction
    {
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
        /// Abort the transaction and roll back any changes. No-op if closed.
        /// Any open <see cref="IResult"/> created by the transaction will be invalidated.
        /// </summary>
        public void Abort()
        {
            if (!this.isClosed)
            {
                this.isClosed = true;
                this.session.AbortTransaction();
            }
        }

        /// <summary>
        /// Commit the transaction. Any open <see cref="IResult"/> created by the transaction will be invalidated.
        /// </summary>
        ///
        /// <exception cref="InvalidOperationException">Thrown when Hash returned from QLDB is not equal.</exception>
        /// <exception cref="OccConflictException">Thrown if an OCC conflict has been detected within the transaction.</exception>
        /// <exception cref="AmazonServiceException">Thrown when there is an error committing this transaction against QLDB.</exception>
        /// <exception cref="QldbDriverException">Thrown when this transaction has been disposed.</exception>
        public void Commit()
        {
            try
            {
                byte[] hashBytes = this.qldbHash.Hash;
                MemoryStream commitDigest = this.session.CommitTransaction(this.txnId, new MemoryStream(hashBytes))
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
                this.Dispose();
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
        public void Dispose()
        {
            try
            {
                this.Abort();
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
        ///
        /// <returns>Result from executed statement.</returns>
        ///
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        /// <exception cref="QldbDriverException">Thrown when this transaction has been disposed.</exception>
        public IResult Execute(string statement)
        {
            return this.Execute(statement, new List<IIonValue>());
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
        /// <exception cref="QldbDriverException">Thrown when this transaction has been disposed.</exception>
        public IResult Execute(string statement, List<IIonValue> parameters)
        {
            ValidationUtils.AssertStringNotEmpty(statement, "statement");

            if (parameters == null)
            {
                parameters = new List<IIonValue>();
            }

            this.qldbHash = Dot(this.qldbHash, statement, parameters);
            ExecuteStatementResult executeStatementResult = this.session.ExecuteStatement(
                this.txnId, statement, parameters);
            return new Result(this.session, this.txnId, executeStatementResult);
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
        /// <exception cref="QldbDriverException">Thrown when this transaction has been disposed.</exception>
        public IResult Execute(string statement, params IIonValue[] parameters)
        {
            return this.Execute(statement, new List<IIonValue>(parameters));
        }
    }
}
