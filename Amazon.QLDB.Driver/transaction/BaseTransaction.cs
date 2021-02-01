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
    internal abstract class BaseTransaction
    {
        protected readonly Session session;
        protected readonly string txnId;
        protected readonly ILogger logger;
        protected QldbHash qldbHash;
        protected bool isClosed = false;

        /// <summary>
        /// Gets the transaction ID.
        /// </summary>
        public string Id => this.txnId;

        /// <summary>
        /// Abort the transaction and roll back any changes. No-op if closed.
        /// Any open <see cref="IResult"/> created by the transaction will be invalidated.
        /// </summary>
        public abstract void Abort();

        /// <summary>
        /// Commit the transaction. Any open <see cref="IResult"/> created by the transaction will be invalidated.
        /// </summary>
        ///
        /// <exception cref="InvalidOperationException">Thrown when Hash returned from QLDB is not equal.</exception>
        /// <exception cref="OccConflictException">Thrown if an OCC conflict has been detected within the transaction.</exception>
        /// <exception cref="AmazonServiceException">Thrown when there is an error committing this transaction against QLDB.</exception>
        /// <exception cref="QldbDriverException">Thrown when this transaction has been disposed.</exception>
        public abstract void Commit();

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
