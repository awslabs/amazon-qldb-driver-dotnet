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
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using IonDotnet.Tree;
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
        /// Aborts the transaction.
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
        /// Commit transaction.
        /// </summary>
        ///
        /// <exception cref="InvalidOperationException">Hash digest not equal.</exception>
        /// <exception cref="OccConflictException">OCC conflict.</exception>
        /// <exception cref="AmazonClientException">Communication issue with QLDB.</exception>
        public void Commit()
        {
            this.ThrowIfClosed();

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
            catch (AmazonClientException ace)
            {
                this.Dispose();
                throw ace;
            }
            finally
            {
                this.isClosed = true;
            }
        }

        /// <summary>
        /// Dispose transaction.
        /// </summary>
        public void Dispose()
        {
            try
            {
                this.Abort();
            }
            catch (AmazonClientException ace)
            {
                this.logger.LogWarning("Ignored AmazonClientException aborting transaction when calling dispose: {}", ace);
            }
        }

        /// <summary>
        /// Executes PartiQL statement.
        /// </summary>
        ///
        /// <param name="statement">PartiQL statement.</param>
        /// <param name="parameters">Ion value parameters.</param>
        ///
        /// <returns>Result from executed statement.</returns>
        public IResult Execute(string statement, List<IIonValue> parameters = null)
        {
            this.ThrowIfClosed();
            ValidationUtils.AssertStringNotEmpty(statement, "statement");

            if (parameters == null)
            {
                parameters = new List<IIonValue>();
            }

            this.qldbHash = Dot(this.qldbHash, statement, parameters);
            ExecuteStatementResult executeStatementResult = this.session.ExecuteStatement(
                this.txnId, statement, parameters);
            return new Result(this.session, this.txnId, executeStatementResult.FirstPage);
        }

        /// <summary>
        /// Calculates the QLDB hash from statement and parameters.
        /// </summary>
        ///
        /// <param name="seed">QLDB Hash.</param>
        /// <param name="statement">PartiQL statement.</param>
        /// <param name="parameters">Ion value parameters.</param>
        ///
        /// <returns>QLDB hash.</returns>
        private static QldbHash Dot(QldbHash seed, string statement, List<IIonValue> parameters)
        {
            QldbHash statementHash = QldbHash.ToQldbHash(statement);
            foreach (var ionValue in parameters)
            {
                statementHash = statementHash.Dot(QldbHash.ToQldbHash(ionValue));
            }

            return seed.Dot(statementHash);
        }

        /// <summary>
        /// Throws exception if the transaction is closed.
        /// </summary>
        ///
        /// <exception cref="ObjectDisposedException">Transaction is closed.</exception>
        private void ThrowIfClosed()
        {
            if (this.isClosed)
            {
                throw new ObjectDisposedException(ExceptionMessages.TransactionClosed);
            }
        }
    }
}
