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
    using Amazon.QLDBSession;
    using Amazon.Runtime;
    using IonDotnet.Tree;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Abstract base class for QldbDriver objects.
    /// </summary>
    public abstract class BaseQldbDriver : IQldbDriver
    {
#pragma warning disable SA1600 // Elements should be documented
        private protected readonly string ledgerName;
        private protected readonly AmazonQLDBSessionClient sessionClient;
        private protected readonly int retryLimit;
        private protected readonly ILogger logger;
        private protected bool isClosed = false;
#pragma warning restore SA1600 // Elements should be documented

        /// <summary>
        /// Initializes a new instance of the <see cref="BaseQldbDriver"/> class.
        /// </summary>
        ///
        /// <param name="ledgerName">The ledger to create sessions to.</param>
        /// <param name="sessionClient">QLDB session client.</param>
        /// <param name="retryLimit">The amount of retries sessions created by this driver will attempt upon encountering a non-fatal error.</param>
        /// <param name="logger">Logger to be used by this.</param>
        internal BaseQldbDriver(string ledgerName, AmazonQLDBSessionClient sessionClient, int retryLimit, ILogger logger)
        {
            this.ledgerName = ledgerName;
            this.sessionClient = sessionClient;
            this.retryLimit = retryLimit;
            this.logger = logger;
        }

        /// <summary>
        /// Start a session, then execute the statement against QLDB and retrieve the result, and end the session.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        ///
        /// <returns>The result of executing the statement.</returns>
        ///
        /// <exception cref="ObjectDisposedException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonClientException">Thrown when there is an error executing against QLDB.</exception>
        public IResult Execute(string statement)
        {
            using (var session = this.GetSession())
            {
                return session.Execute(statement);
            }
        }

        /// <summary>
        /// Start a session, then execute the statement against QLDB and retrieve the result, and end the session.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="retryAction">A lambda that is invoked when the statement execution is about to be retried due to
        /// a retriable error. Can be null if not applicable.</param>
        ///
        ///
        /// <returns>The result of executing the statement.</returns>
        ///
        /// <exception cref="ObjectDisposedException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonClientException">Thrown when there is an error executing against QLDB.</exception>
        public IResult Execute(string statement, Action<int> retryAction)
        {
            using (var session = this.GetSession())
            {
                return session.Execute(statement, retryAction);
            }
        }

        /// <summary>
        /// Start a session, then execute the statement using the specified parameters against QLDB and retrieve the
        /// result, and end the session.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="parameters">Parameters to execute.</param>
        ///
        /// <returns>The result of executing the statement.</returns>
        ///
        /// <exception cref="ObjectDisposedException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonClientException">Thrown when there is an error executing against QLDB.</exception>
        public IResult Execute(string statement, List<IIonValue> parameters)
        {
            using (var session = this.GetSession())
            {
                return session.Execute(statement, parameters);
            }
        }

        /// <summary>
        /// Start a session, then execute the statement using the specified parameters against QLDB and retrieve the
        /// result, and end the session.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="parameters">Parameters to execute.</param>
        ///
        /// <returns>The result of executing the statement.</returns>
        ///
        /// <exception cref="ObjectDisposedException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonClientException">Thrown when there is an error executing against QLDB.</exception>
        public IResult Execute(string statement, params IIonValue[] parameters)
        {
            using (var session = this.GetSession())
            {
                return session.Execute(statement, parameters);
            }
        }

        /// <summary>
        /// Start a session, then execute the statement using the specified parameters against QLDB and retrieve the
        /// result, and end the session.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="retryAction">A lambda that is invoked when the statement execution is about to be retried due to
        /// a retriable error. Can be null if not applicable.</param>
        /// <param name="parameters">Parameters to execute.</param>
        ///
        /// <returns>The result of executing the statement.</returns>
        ///
        /// <exception cref="ObjectDisposedException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonClientException">Thrown when there is an error executing against QLDB.</exception>
        public IResult Execute(string statement, Action<int> retryAction, List<IIonValue> parameters)
        {
            using (var session = this.GetSession())
            {
                return session.Execute(statement, retryAction, parameters);
            }
        }

        /// <summary>
        /// Start a session, then execute the statement using the specified parameters against QLDB and retrieve the
        /// result, and end the session.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="retryAction">A lambda that is invoked when the statement execution is about to be retried due to
        /// a retriable error. Can be null if not applicable.</param>
        /// <param name="parameters">Parameters to execute.</param>
        ///
        /// <returns>The result of executing the statement.</returns>
        ///
        /// <exception cref="ObjectDisposedException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonClientException">Thrown when there is an error executing against QLDB.</exception>
        public IResult Execute(string statement, Action<int> retryAction, params IIonValue[] parameters)
        {
            using (var session = this.GetSession())
            {
                return session.Execute(statement, retryAction, parameters);
            }
        }

        /// <summary>
        /// Start a session, then execute the Executor lambda against QLDB within a transaction where no result is expected,
        /// and end the session.
        /// </summary>
        ///
        /// <param name="action">The Executor lambda with no return value representing the block of code to be executed within the transaction.
        /// This cannot have any side effects as it may be invoked multiple times.</param>
        ///
        /// <exception cref="AbortException">Thrown if the Executor lambda calls <see cref="TransactionExecutor.Abort"/>.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonClientException">Thrown when there is an error executing against QLDB.</exception>
        public void Execute(Action<TransactionExecutor> action)
        {
            using (var session = this.GetSession())
            {
                session.Execute(action);
            }
        }

        /// <summary>
        /// Start a session, then execute the Executor lambda against QLDB within a transaction where no result is expected,
        /// and end the session.
        /// </summary>
        ///
        /// <param name="action">The Executor lambda with no return value representing the block of code to be executed within the transaction.
        /// This cannot have any side effects as it may be invoked multiple times.</param>
        /// <param name="retryAction">A lambda that is invoked when the Executor lambda is about to be retried due to
        /// a retriable error. Can be null if not applicable.</param>
        ///
        /// <exception cref="AbortException">Thrown if the Executor lambda calls <see cref="TransactionExecutor.Abort"/>.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonClientException">Thrown when there is an error executing against QLDB.</exception>
        public void Execute(Action<TransactionExecutor> action, Action<int> retryAction)
        {
            using (var session = this.GetSession())
            {
                session.Execute(action, retryAction);
            }
        }

        /// <summary>
        /// Start a session, then execute the Executor lambda against QLDB and retrieve the result within a transaction,
        /// and end the session.
        /// </summary>
        ///
        /// <param name="func">The Executor lambda representing the block of code to be executed within the transaction. This cannot have any
        /// side effects as it may be invoked multiple times, and the result cannot be trusted until the
        /// transaction is committed.</param>
        /// <typeparam name="T">The return type.</typeparam>
        ///
        /// <returns>The return value of executing the executor. Note that if you directly return a <see cref="IResult"/>, this will
        /// be automatically buffered in memory before the implicit commit to allow reading, as the commit will close
        /// any open results. Any other <see cref="IResult"/> instances created within the executor block will be
        /// invalidated, including if the return value is an object which nests said <see cref="IResult"/> instances within it.
        /// </returns>
        ///
        /// <exception cref="AbortException">Thrown if the Executor lambda calls <see cref="TransactionExecutor.Abort"/>.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonClientException">Thrown when there is an error executing against QLDB.</exception>
        public T Execute<T>(Func<TransactionExecutor, T> func)
        {
            using (var session = this.GetSession())
            {
                return session.Execute(func);
            }
        }

        /// <summary>
        /// Start a session, then execute the Executor lambda against QLDB and retrieve the result within a transaction,
        /// and end the session.
        /// </summary>
        ///
        /// <param name="func">The Executor lambda representing the block of code to be executed within the transaction. This cannot have any
        /// side effects as it may be invoked multiple times, and the result cannot be trusted until the
        /// transaction is committed.</param>
        /// <param name="retryAction">A lambda that is invoked when the Executor lambda is about to be retried due to
        /// a retriable error. Can be null if not applicable.</param>
        /// <typeparam name="T">The return type.</typeparam>
        ///
        /// <returns>The return value of executing the executor. Note that if you directly return a <see cref="IResult"/>, this will
        /// be automatically buffered in memory before the implicit commit to allow reading, as the commit will close
        /// any open results. Any other <see cref="IResult"/> instances created within the executor block will be
        /// invalidated, including if the return value is an object which nests said <see cref="IResult"/> instances within it.
        /// </returns>
        ///
        /// <exception cref="AbortException">Thrown if the Executor lambda calls <see cref="TransactionExecutor.Abort"/>.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonClientException">Thrown when there is an error executing against QLDB.</exception>
        public T Execute<T>(Func<TransactionExecutor, T> func, Action<int> retryAction)
        {
            using (var session = this.GetSession())
            {
                return session.Execute(func, retryAction);
            }
        }

        /// <summary>
        /// Close this driver. No-op if already closed.
        /// </summary>
        public abstract void Dispose();

        /// <summary>
        /// <para>Get a <see cref="IQldbSession"/> object.</para>
        /// </summary>
        ///
        /// <returns>The <see cref="IQldbSession"/> object.</returns>
        public abstract IQldbSession GetSession();
    }
}
