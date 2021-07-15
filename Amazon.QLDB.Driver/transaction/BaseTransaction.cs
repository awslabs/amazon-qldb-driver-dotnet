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
    using System.Collections.Generic;
    using Amazon.IonDotnet.Tree;
    using Amazon.QLDBSession.Model;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Base class for Transaction.
    /// </summary>
    internal abstract class BaseTransaction
    {
        protected readonly Session session;
        protected readonly string txnId;
        protected readonly ILogger logger;
        protected QldbHash qldbHash;

        /// <summary>
        /// Initializes a new instance of the <see cref="BaseTransaction"/> class.
        /// </summary>
        ///
        /// <param name="session">The parent session that represents the communication channel to QLDB.</param>
        /// <param name="txnId">Transaction identifier.</param>
        /// <param name="logger">Logger to be used by this.</param>
        internal BaseTransaction(Session session, string txnId, ILogger logger)
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

        internal static QldbHash Dot(QldbHash seed, string statement, ValueHolder[] parameters)
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
