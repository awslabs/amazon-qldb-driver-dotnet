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
    using Amazon.IonDotnet.Tree;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Represents a pooled session object. See <see cref="QldbSession"/> for more details.
    /// </summary>
    internal class PooledQldbSession : IQldbSession
    {
        private readonly QldbSession session;
        private readonly Action<QldbSession> disposeDelegate;
        private readonly ILogger logger;
        private bool isClosed;

        /// <summary>
        /// Initializes a new instance of the <see cref="PooledQldbSession"/> class.
        /// </summary>
        ///
        /// <param name="qldbSession">The QldbSession instance that this wraps.</param>
        /// <param name="disposeDelegate">The delegate method to invoke upon disposal of this.</param>
        /// <param name="logger">The logger to be used by this.</param>
        internal PooledQldbSession(QldbSession qldbSession, Action<QldbSession> disposeDelegate, ILogger logger)
        {
            this.session = qldbSession;
            this.disposeDelegate = disposeDelegate;
            this.logger = logger;
            this.isClosed = false;
        }

        /// <summary>
        /// Close this session and return it to the pool. No-op if already closed.
        /// </summary>
        public void Dispose()
        {
            if (!this.isClosed)
            {
                this.isClosed = true;
                this.disposeDelegate(this.session);
            }
        }

        /// <summary>
        /// Execute the statement against QLDB and retrieve the result.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="parameters">Parameters to execute.</param>
        /// <param name="retryAction">A lambda that which is invoked when the Executor lambda is about to be retried due to
        /// a retriable error. Can be null if not applicable.</param>
        ///
        /// <returns>The result of executing the statement.</returns>
        ///
        /// <exception cref="AbortException">If the Executor lambda calls <see cref="TransactionExecutor.Abort"/>.</exception>
        /// <exception cref="InvalidSessionException">Thrown when the session retry limit is reached.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when session is closed.</exception>
        /// <exception cref="OccConflictException">If the number of retries has exceeded the limit and an OCC conflict occurs.</exception>
        /// <exception cref="AmazonClientException">If there is an error communicating with QLDB.</exception>
        public IResult Execute(string statement, List<IIonValue> parameters = null, Action<int> retryAction = null)
        {
            this.ThrowIfClosed();
            return this.session.Execute(statement, parameters, retryAction);
        }

        /// <summary>
        /// Execute the Executor lambda against QLDB within a transaction where no result is expected.
        /// </summary>
        ///
        /// <param name="action">A lambda with no return value representing the block of code to be executed within the transaction.
        /// This cannot have any side effects as it may be invoked multiple times.</param>
        /// <param name="retryAction">A lambda that which is invoked when the Executor lambda is about to be retried due to
        /// a retriable error. Can be null if not applicable.</param>
        ///
        /// <exception cref="AbortException">If the Executor lambda calls <see cref="TransactionExecutor.Abort"/>.</exception>
        /// <exception cref="InvalidSessionException">Thrown when the session retry limit is reached.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when session is closed.</exception>
        /// <exception cref="OccConflictException">If the number of retries has exceeded the limit and an OCC conflict occurs.</exception>
        /// <exception cref="AmazonClientException">If there is an error communicating with QLDB.</exception>
        public void Execute(Action<TransactionExecutor> action, Action<int> retryAction = null)
        {
            this.ThrowIfClosed();
            this.session.Execute(action, retryAction);
        }

        /// <summary>
        /// Execute the Executor lambda against QLDB and retrieve the result within a transaction.
        /// </summary>
        ///
        /// <param name="func">
        /// A lambda representing the block of code to be executed within the transaction. This cannot have any
        /// side effects as it may be invoked multiple times, and the result cannot be trusted until the
        /// transaction is committed.</param>
        /// <param name="retryAction">A lambda that which is invoked when the Executor lambda is about to be retried due to
        /// a retriable error. Can be null if not applicable.</param>
        /// <typeparam name="T">The return type.</typeparam>
        ///
        /// <returns>The return value of executing the executor. Note that if you directly return a <see cref="IResult"/>, this will
        /// be automatically buffered in memory before the implicit commit to allow reading, as the commit will close
        /// any open results. Any other <see cref="IResult"/> instances created within the executor block will be
        /// invalidated, including if the return value is an object which nests said <see cref="IResult"/> instances within it.</returns>
        ///
        /// <exception cref="AbortException">If the Executor lambda calls <see cref="TransactionExecutor.Abort"/>.</exception>
        /// <exception cref="InvalidSessionException">Thrown when the session retry limit is reached.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when session is closed.</exception>
        /// <exception cref="OccConflictException">If the number of retries has exceeded the limit and an OCC conflict occurs.</exception>
        /// <exception cref="AmazonClientException">If there is an error communicating with QLDB.</exception>
        public T Execute<T>(Func<TransactionExecutor, T> func, Action<int> retryAction = null)
        {
            this.ThrowIfClosed();
            return this.session.Execute(func, retryAction);
        }

        /// <summary>
        /// Retrieve the table names that are available within the ledger.
        /// </summary>
        ///
        /// <returns>The Enumerable over the table names in the ledger.</returns>
        public IEnumerable<string> ListTableNames()
        {
            this.ThrowIfClosed();
            return this.session.ListTableNames();
        }

        /// <summary>
        /// Create a transaction object which allows for granular control over when a transaction is aborted or committed.
        /// </summary>
        ///
        /// <returns>The newly created transaction object.</returns>
        public ITransaction StartTransaction()
        {
            this.ThrowIfClosed();
            return this.session.StartTransaction();
        }

        private void ThrowIfClosed()
        {
            if (this.isClosed)
            {
                this.logger.LogError(ExceptionMessages.SessionClosed);
                throw new ObjectDisposedException(ExceptionMessages.SessionClosed);
            }
        }
    }
}
