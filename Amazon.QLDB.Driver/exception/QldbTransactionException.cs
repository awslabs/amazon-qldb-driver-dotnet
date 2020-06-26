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

    /// <summary>
    /// The root exception for all transaction related exceptions. It reports back the
    /// transaction ID.
    /// </summary>
    public class QldbTransactionException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="QldbTransactionException"/> class.
        /// </summary>
        /// <param name="transactionId">The transaction ID.</param>
        public QldbTransactionException(string transactionId)
        {
            this.TransactionId = transactionId;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QldbTransactionException"/> class.
        /// </summary>
        /// <param name="transactionId">The transaction ID.</param>
        /// <param name="innerException">The inner exception.</param>
        public QldbTransactionException(string transactionId, Exception innerException)
            : base("QLDB Transaction Exception.", innerException)
        {
            this.TransactionId = transactionId;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QldbTransactionException"/> class.
        /// </summary>
        /// <param name="transactionId">The transaction ID.</param>
        /// <param name="innerException">The inner exception.</param>
        /// <param name="errorMessage">The customized error message.</param>
        public QldbTransactionException(string errorMessage, string transactionId, Exception innerException)
           : base(errorMessage, innerException)
        {
            this.TransactionId = transactionId;
        }

        /// <summary>
        /// Gets the transaction ID.
        /// </summary>
        public string TransactionId { get; }
    }
}
