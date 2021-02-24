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
    using System.Collections.Generic;
    using System.Linq;
    using Amazon.Runtime;

    /// <summary>
    /// <para>Represents a factory for accessing a specific ledger within QLDB. This class or
    /// <see cref="QldbDriver"/> should be the main entry points to any interaction with QLDB.
    ///
    /// <para>This factory pools sessions and attempts to return unused but available sessions when getting new sessions.
    /// The pool does not remove stale sessions until a new session is retrieved. The default pool size is the maximum
    /// amount of connections the session client allows set in the <see cref="ClientConfig"/>. <see cref="Dispose"/>
    /// should be called when this factory is no longer needed in order to clean up resources, ending all sessions in the pool.</para>
    /// </summary>
    public class QldbDriver : BaseQldbDriver, IQldbDriver
    {
        private readonly SessionPool sessionPool;

        /// <summary>
        /// Initializes a new instance of the <see cref="QldbDriver"/> class.
        /// </summary>
        ///
        /// <param name="sessionPool">The ledger to create sessions to.</param>
        internal QldbDriver(SessionPool sessionPool)
        {
            this.sessionPool = sessionPool;
        }

        /// <summary>
        /// Retrieve a builder object for creating a <see cref="QldbDriver"/>.
        /// </summary>
        ///
        /// <returns>The builder object for creating a <see cref="QldbDriver"/>.</returns>.
        public static QldbDriverBuilder Builder()
        {
            return new QldbDriverBuilder();
        }

        /// <summary>
        /// Start a session, then execute the Executor lambda against QLDB within a transaction where no result is expected,
        /// and end the session.
        /// </summary>
        ///
        /// <param name="action">The Executor lambda with no return value representing the block of code to be executed within the transaction.
        /// This cannot have any side effects as it may be invoked multiple times.</param>
        ///
        /// <exception cref="TransactionAbortedException">Thrown if the Executor lambda calls <see cref="TransactionExecutor.Abort"/>.</exception>
        /// <exception cref="QldbDriverException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        public void Execute(Action<TransactionExecutor> action)
        {
            this.Execute(action, DefaultRetryPolicy);
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
        /// <exception cref="TransactionAbortedException">Thrown if the Executor lambda calls <see cref="TransactionExecutor.Abort"/>.</exception>
        /// <exception cref="QldbDriverException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception
        [Obsolete("As of release 1.0, replaced by 'retryPolicy'. Will be removed in the next major release.")]
        public void Execute(Action<TransactionExecutor> action, Action<int> retryAction)
        {
            this.Execute(
                txn =>
                {
                    action.Invoke(txn);
                    return false;
                },
                retryAction);
        }

        /// <summary>
        /// Start a session, then execute the Executor lambda against QLDB within a transaction where no result is expected,
        /// and end the session.
        /// </summary>
        ///
        /// <param name="action">The Executor lambda with no return value representing the block of code to be executed within the transaction.
        /// This cannot have any side effects as it may be invoked multiple times.</param>
        /// <param name="retryPolicy">A <see cref="RetryPolicy"/> that overrides the RetryPolicy set when creating the driver. The given retry policy
        /// will be used when retrying the transaction.</param>
        ///
        /// <exception cref="TransactionAbortedException">Thrown if the Executor lambda calls <see cref="TransactionExecutor.Abort"/>.</exception>
        /// <exception cref="QldbDriverException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        public void Execute(Action<TransactionExecutor> action, RetryPolicy retryPolicy)
        {
            this.Execute(
                txn =>
                {
                    action.Invoke(txn);
                    return false;
                },
                retryPolicy);
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
        /// <exception cref="TransactionAbortedException">Thrown if the Executor lambda calls <see cref="TransactionExecutor.Abort"/>.</exception>
        /// <exception cref="QldbDriverException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        public T Execute<T>(Func<TransactionExecutor, T> func)
        {
            return this.Execute(func, DefaultRetryPolicy);
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
        /// <exception cref="TransactionAbortedException">Thrown if the Executor lambda calls <see cref="TransactionExecutor.Abort"/>.</exception>
        /// <exception cref="QldbDriverException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        [Obsolete("As of release 1.0, replaced by 'retryPolicy'. Will be removed in the next major release.")]
        public T Execute<T>(Func<TransactionExecutor, T> func, Action<int> retryAction)
        {
            return this.Execute(func, DefaultRetryPolicy, retryAction);
        }

        /// <summary>
        /// Start a session, then execute the Executor lambda against QLDB and retrieve the result within a transaction,
        /// and end the session.
        /// </summary>
        ///
        /// <param name="func">The Executor lambda representing the block of code to be executed within the transaction. This cannot have any
        /// side effects as it may be invoked multiple times, and the result cannot be trusted until the
        /// transaction is committed.</param>
        /// <param name="retryPolicy">A <see cref="RetryPolicy"/> that overrides the RetryPolicy set when creating the driver. The given retry policy
        /// will be used when retrying the transaction.</param>
        /// <typeparam name="T">The return type.</typeparam>
        ///
        /// <returns>The return value of executing the executor. Note that if you directly return a <see cref="IResult"/>, this will
        /// be automatically buffered in memory before the implicit commit to allow reading, as the commit will close
        /// any open results. Any other <see cref="IResult"/> instances created within the executor block will be
        /// invalidated, including if the return value is an object which nests said <see cref="IResult"/> instances within it.
        /// </returns>
        ///
        /// <exception cref="TransactionAbortedException">Thrown if the Executor lambda calls <see cref="TransactionExecutor.Abort"/>.</exception>
        /// <exception cref="QldbDriverException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        public T Execute<T>(Func<TransactionExecutor, T> func, RetryPolicy retryPolicy)
        {
            return this.Execute(func, retryPolicy, null);
        }

        /// <summary>
        /// Retrieve the table names that are available within the ledger.
        /// </summary>
        ///
        /// <returns>The Enumerable over the table names in the ledger.</returns>
        public IEnumerable<string> ListTableNames()
        {
            return this.Execute((txn) =>
            {
                return txn.Execute(TableNameQuery);
            }).Select(i => i.StringValue);
        }

        /// <summary>
        /// Close this driver and end all sessions in the current pool. No-op if already closed.
        /// </summary>
        public void Dispose()
        {
            this.sessionPool.Dispose();
        }

        internal T Execute<T>(Func<TransactionExecutor, T> func, RetryPolicy retryPolicy, Action<int> retryAction)
        {
            return this.sessionPool.Execute(func, retryPolicy, retryAction);
        }
    }
}
