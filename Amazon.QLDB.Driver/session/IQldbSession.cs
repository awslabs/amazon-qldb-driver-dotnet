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
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using IonDotnet.Tree;

    /// <summary>
    /// <para>The top-level interface for a QldbSession object for interacting with QLDB. A QldbSession is linked to the
    /// specified ledger in the parent driver of the instance of the QldbSession. In any given QldbSession, only one
    /// transaction can be active at a time. This object can have only one underlying session to QLDB, and therefore the
    /// lifespan of a QldbSession is tied to the underlying session, which is not indefinite, and on expiry this QldbSession
    /// will become invalid. A new <see cref="IQldbSession"/> needs to be created from the parent driver in order to continue usage.</para>
    ///
    /// <para>When a <see cref="IQldbSession"/> is no longer needed, <see cref="IDisposable.Dispose"/> should be invoked in order to clean up any resources.</para>
    ///
    /// <para>See <see cref="PooledQldbDriver"/> for an example of session lifecycle management, allowing the re-use of sessions when
    /// possible. There should only be one thread interacting with a session at any given time.</para>
    ///
    /// <para>There are three methods of execution, ranging from simple to complex; the first two are recommended for inbuilt
    /// error handling:
    /// <list type="bullet">
    /// <item><description>Execute(string) and Execute(string, List) allow for a single statement to be executed within a
    /// transaction where the transaction is implicitly created and committed, and any recoverable errors are transparently handled.</description></item>
    /// <item><description>Execute(Action, Action) and Execute(Func, Action) allow for more complex execution sequences where
    /// more than one execution can occur, as well as other method calls. The transaction is implicitly created and committed, and any
    /// recoverable errors are transparently handled.</description></item>
    /// <item><description>Execute(Action) and Execute(Func) are also available, providing the same functionality as the former
    /// two functions, but without a lambda function to be invoked upon a retriable error.</description></item>
    /// <item><description><see cref="StartTransaction"/> allows for full control over when the transaction is committed and leaves the
    /// responsibility of OCC conflict handling up to the user. Transaction methods cannot be automatically retried, as
    /// the state of the transaction is ambiguous in the case of an unexpected error.</description></item>
    /// </list>
    /// </para>
    /// </summary>
    public interface IQldbSession : IDisposable
    {
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
        IResult Execute(string statement, List<IIonValue> parameters = null, Action<int> retryAction = null);

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
        void Execute(Action<TransactionExecutor> action, Action<int> retryAction = null);

        /// <summary>
        /// Execute the Executor lambda against QLDB and retrieve the result within a transaction.
        /// </summary>
        ///
        /// <param name="func">A lambda representing the block of code to be executed within the transaction. This cannot have any
        /// side effects as it may be invoked multiple times, and the result cannot be trusted until the
        /// transaction is committed.</param>
        /// <param name="retryAction">A lambda that which is invoked when the Executor lambda is about to be retried due to
        /// a retriable error. Can be null if not applicable.</param>
        /// <typeparam name="T">The return type.</typeparam>
        ///
        /// <returns>The return value of executing the executor. Note that if you directly return a <see cref="IResult"/>, this will
        /// be automatically buffered in memory before the implicit commit to allow reading, as the commit will close
        /// any open results. Any other <see cref="IResult"/> instances created within the executor block will be
        /// invalidated, including if the return value is an object which nests said <see cref="IResult"/> instances within it.
        /// </returns>
        ///
        /// <exception cref="AbortException">If the Executor lambda calls <see cref="TransactionExecutor.Abort"/>.</exception>
        /// <exception cref="InvalidSessionException">Thrown when the session retry limit is reached.</exception>
        /// <exception cref="ObjectDisposedException">Thrown when session is closed.</exception>
        /// <exception cref="OccConflictException">If the number of retries has exceeded the limit and an OCC conflict occurs.</exception>
        /// <exception cref="AmazonClientException">If there is an error communicating with QLDB.</exception>
        T Execute<T>(Func<TransactionExecutor, T> func, Action<int> retryAction = null);

        /// <summary>
        /// Retrieve the table names that are available within the ledger.
        /// </summary>
        ///
        /// <returns>The Enumerable over the table names in the ledger.</returns>
        IEnumerable<string> ListTableNames();

        /// <summary>
        /// Create a transaction object which allows for granular control over when a transaction is aborted or committed.
        /// </summary>
        ///
        /// <returns>The newly created transaction object.</returns>
        ITransaction StartTransaction();
    }
}
