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
    /// This exception is used internally to allow lower level exceptions to be retried. In some cases, only
    /// the instances of an exception class with certain data values, e.g. HTTP Status code, are allowed
    /// to be retried, so we wrapped them into this exception.
    /// </summary>
    internal class RetriableException : QldbTransactionException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="RetriableException"/> class.
        /// </summary>
        /// <param name="transactionId">The transaction ID.</param>
        /// <param name="innerException">The exception that can be retried.</param>
        public RetriableException(string transactionId, Exception innerException)
            : base("QLDB retriable exception.", transactionId, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RetriableException"/> class.
        /// </summary>
        /// <param name="transactionId">The transaction ID.</param>
        /// <param name="isSessionAlive">Whether the session is still alive.</param>
        /// <param name="innerException">The exception that can be retried.</param>
        public RetriableException(string transactionId, bool isSessionAlive, Exception innerException)
            : base("Qldb retriable exception.", transactionId, isSessionAlive, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RetriableException"/> class.
        /// </summary>
        /// <param name="message">Error message.</param>
        /// <param name="transactionId">The transaction ID.</param>
        /// <param name="isSessionAlive">Whether the session is still alive.</param>
        /// <param name="innerException">The exception that can be retried.</param>
        public RetriableException(string message, string transactionId, bool isSessionAlive, Exception innerException)
            : base(message, transactionId, isSessionAlive, innerException)
        {
        }
    }
}
