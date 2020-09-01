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
    /// Exception thrown when an attempt is made to start another transaction on the same session
    /// while the previous transaction was still open.
    /// </summary>
    public class TransactionAlreadyOpenException : RetriableException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionAlreadyOpenException"/> class.
        /// </summary>
        /// <param name="transactionId">The transaction ID.</param>
        /// <param name="isSessionAlive">Whether the sesion is still a live.</param>
        /// <param name="innerException">The inner exception.</param>
        public TransactionAlreadyOpenException(string transactionId, bool isSessionAlive, Exception innerException)
            : base(ExceptionMessages.TransactionAlreadyOpened, transactionId, isSessionAlive, innerException)
        {
        }
    }
}
