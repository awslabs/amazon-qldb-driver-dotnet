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

    /// <summary>
    /// The root exception for all transaction related exceptions. It reports back the
    /// transaction ID.
    /// </summary>
    public class QldbTransactionException : Exception
    {
        internal const string DefaultTransactionId = "None";

        /// <summary>
        /// Initializes a new instance of the <see cref="QldbTransactionException"/> class.
        /// </summary>
        /// <param name="transactionId">The transaction ID.</param>
        public QldbTransactionException(string transactionId)
            : this(transactionId, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QldbTransactionException"/> class.
        /// </summary>
        /// <param name="transactionId">The transaction ID.</param>
        /// <param name="innerException">The inner exception.</param>
        public QldbTransactionException(string transactionId, Exception innerException)
            : this("QLDB Transaction Exception.", transactionId, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QldbTransactionException"/> class.
        /// </summary>
        /// <param name="errorMessage">The customized error message.</param>
        /// <param name="transactionId">The transaction ID.</param>
        /// <param name="innerException">The inner exception.</param>
        public QldbTransactionException(string errorMessage, string transactionId, Exception innerException)
           : this(errorMessage, transactionId, true, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QldbTransactionException"/> class.
        /// </summary>
        /// <param name="transactionId">The transaction ID.</param>
        /// <param name="isSessionAlive">Whether the session is still alive.</param>
        public QldbTransactionException(string transactionId, bool isSessionAlive)
            : this("QLDB Transaction Exception.", transactionId, isSessionAlive, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QldbTransactionException"/> class.
        /// </summary>
        /// <param name="transactionId">The transaction ID.</param>
        /// <param name="isSessionAlive">Whether the session is still alive.</param>
        /// <param name="innerException">The inner exception.</param>
        public QldbTransactionException(string transactionId, bool isSessionAlive, Exception innerException)
            : this("QLDB Transaction Exception.", transactionId, isSessionAlive, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QldbTransactionException"/> class.
        /// </summary>
        /// <param name="errorMessage">The customized error message.</param>
        /// <param name="transactionId">The transaction ID.</param>
        /// <param name="isSessionAlive">Whether the session is still alive.</param>
        /// <param name="innerException">The inner exception.</param>
        public QldbTransactionException(
            string errorMessage,
            string transactionId,
            bool isSessionAlive,
            Exception innerException)
           : base(errorMessage, innerException)
        {
            this.TransactionId = transactionId;
            this.IsSessionAlive = isSessionAlive;
        }

        /// <summary>
        /// Gets the transaction ID.
        /// </summary>
        public string TransactionId { get; }

        /// <summary>
        /// Gets a value indicating whether the session is still alive after this exception.
        /// </summary>
        public bool IsSessionAlive { get; }
    }
}
