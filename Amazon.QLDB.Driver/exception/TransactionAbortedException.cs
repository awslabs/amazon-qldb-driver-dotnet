﻿/*
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
    /// Exception type representing the abort of a transaction within a lambda execution block. Signals that the lambda
    /// should cease to execute and the current transaction should be aborted.
    /// </summary>
    public class TransactionAbortedException : QldbTransactionException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionAbortedException"/> class.
        /// </summary>
        /// <param name="transactionId">The transaction ID.</param>
        /// <param name="isSessionAlive">Whether the session is alive.</param>
        public TransactionAbortedException(string transactionId, bool isSessionAlive)
            : base(transactionId, isSessionAlive)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionAbortedException"/> class.
        /// </summary>
        /// <param name="transactionId">The transaction ID.</param>
        /// <param name="isSessionAlive">Whether the session is alive.</param>
        /// <param name="innerException">The inner exception.</param>
        public TransactionAbortedException(string transactionId, bool isSessionAlive, Exception innerException)
            : base(transactionId, isSessionAlive, innerException)
        {
        }
    }
}
