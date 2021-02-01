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
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Base class for QLDB Session
    /// </summary>
    internal abstract class BaseQldbSession
    {
        protected readonly ILogger logger;
        protected readonly Action<QldbSession> releaseSession;
        protected Session session;
        protected bool isAlive;

        public bool IsAlive()
        {
            return this.isAlive;
        }

        /// <summary>
        /// Close the internal session object.
        /// </summary>
        public void Close()
        {
            this.session.End();
        }

        /// <summary>
        /// Release the session which still can be used by another transaction.
        /// </summary>
        public abstract void Release();

        /// <summary>
        /// Create a transaction object which allows for granular control over when a transaction is aborted or committed.
        /// </summary>
        ///
        /// <returns>The newly created transaction object.</returns>
        public virtual ITransaction StartTransaction()
        {
            try
            {
                var startTransactionResult = this.session.StartTransaction();
                return new Transaction(this.session, startTransactionResult.TransactionId, this.logger);
            }
            catch (BadRequestException e)
            {
                throw new QldbTransactionException(ExceptionMessages.TransactionAlreadyOpened, string.Empty, this.TryAbort(null), e);
            }
        }

        /// <summary>
        /// Retrieve the ID of this session..
        /// </summary>
        ///
        /// <returns>The ID of this session.</returns>
        internal string GetSessionId()
        {
            return this.session.SessionId;
        }

        /// <summary>
        /// Try to abort the transaction.
        /// </summary>
        ///
        /// <param name="transaction">The transaction to abort.</param>
        /// <returns>Whether the abort call has succeeded.</returns>
        /// <exception cref="AmazonServiceException">If there is an error communicating with QLDB.</exception>
        protected bool TryAbort(ITransaction transaction)
        {
            try
            {
                if (transaction != null)
                {
                    transaction.Abort();
                }
                else
                {
                    this.session.AbortTransaction();
                }
            }
            catch (AmazonServiceException ase)
            {
                this.logger.LogWarning("This session is invalid on ABORT: {}", ase);
                this.isAlive = false;
                return false;
            }

            return true;
        }
    }
}
