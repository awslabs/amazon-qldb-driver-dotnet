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
    using System.Threading.Tasks;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging;

    internal class AsyncQldbSession : BaseQldbSession
    {
        private readonly ILogger logger;
        private readonly Action<AsyncQldbSession> releaseSession;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncQldbSession"/> class.
        /// </summary>
        ///
        /// <param name="session">The session object representing a communication channel with QLDB.</param>
        /// <param name="releaseSession">The delegate method to release the session.</param>
        /// <param name="logger">The logger to be used by this.</param>
        internal AsyncQldbSession(Session session, Action<AsyncQldbSession> releaseSession, ILogger logger)
        {
            this.session = session;
            this.releaseSession = releaseSession;
            this.logger = logger;
            this.isAlive = true;
        }

        /// <summary>
        /// Release the session which still can be used by another transaction.
        /// </summary>
        public override void Release()
        {
            this.releaseSession(this);
        }

        /// <summary>
        /// Create a transaction object which allows for granular control over when a transaction is aborted or committed.
        /// </summary>
        ///
        /// <returns>The newly created transaction object.</returns>
        public virtual IAsyncTransaction StartTransaction()
        {
            try
            {
                var startTransactionResult = this.session.StartTransaction();
                return new AsyncTransaction(this.session, startTransactionResult.TransactionId, this.logger);
            }
            catch (BadRequestException e)
            {
                throw new QldbTransactionException(ExceptionMessages.TransactionAlreadyOpened, string.Empty, this.TryAbort(null), e);
            }
        }

        /// <summary>
        /// Try to abort the transaction.
        /// </summary>
        ///
        /// <param name="transaction">The transaction to abort.</param>
        /// <returns>Whether the abort call has succeeded.</returns>
        /// <exception cref="AmazonServiceException">If there is an error communicating with QLDB.</exception>
        protected bool TryAbort(IAsyncTransaction transaction)
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

        public async Task<T> Execute<T>(Func<AsyncTransactionExecutor, T> func)
        {
            throw new NotImplementedException();
        }
    }
}
