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
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.Runtime;

    /// <summary>
    /// Interface for the Async QLDB driver.
    /// </summary>
    public interface IAsyncQldbDriver : IDisposable
    {
        /// <summary>
        /// Retrieve the table names that are available within the ledger.
        /// </summary>
        ///
        /// <param name="cancellationToken"> A cancellation token that can be used by other objects or threads to
        /// receive notice of cancellation.</param>
        ///
        /// <returns>The task Enumerable containing the table names in the ledger.</returns>
        Task<IEnumerable<string>> ListTableNames(CancellationToken cancellationToken = default);

        /// <summary>
        /// Execute the Async Executor lambda against QLDB within a transaction where no result is expected.
        /// </summary>
        ///
        /// <param name="action">The Async Executor lambda with no return value representing the block of code to be
        /// executed within the transaction. This cannot have any side effects as it may be invoked multiple times.</param>
        /// <param name="cancellationToken"> A cancellation token that can be used by other objects or threads to
        /// receive notice of cancellation.</param>
        ///
        /// <exception cref="TransactionAbortedException">Thrown if the Executor lambda calls
        /// <see cref="AsyncTransactionExecutor.Abort"/>.</exception>
        /// <exception cref="QldbDriverException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        ///
        /// <returns>No object or value is returned by this method when it completes.</returns>
        Task Execute(Func<AsyncTransactionExecutor, Task> action, CancellationToken cancellationToken = default);

        /// <summary>
        /// Execute the Async Executor lambda against QLDB within a transaction where no result is expected.
        /// </summary>
        ///
        /// <param name="action">The Async Executor lambda with no return value representing the block of code to
        /// be executed within the transaction. This cannot have any side effects as it may be invoked multiple times.</param>
        /// <param name="retryPolicy">A <see cref="RetryPolicy"/> that overrides the RetryPolicy set when creating
        /// the driver. The given retry policy will be used when retrying the transaction.</param>
        /// <param name="cancellationToken"> A cancellation token that can be used by other objects or threads to
        /// receive notice of cancellation.</param>
        ///
        /// <exception cref="TransactionAbortedException">Thrown if the Executor lambda calls
        /// <see cref="AsyncTransactionExecutor.Abort"/>.</exception>
        /// <exception cref="QldbDriverException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        ///
        /// <returns>No object or value is returned by this method when it completes.</returns>
        Task Execute(
            Func<AsyncTransactionExecutor, Task> action,
            RetryPolicy retryPolicy,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Execute the Async Executor lambda against QLDB and retrieve the result within a transaction.
        /// </summary>
        ///
        /// <param name="func">The Async Executor lambda representing the block of code to be executed within the
        /// transaction. This cannot have any side effects as it may be invoked multiple times, and the result cannot
        /// be trusted until the transaction is committed.</param>
        /// <param name="cancellationToken"> A cancellation token that can be used by other objects or threads to
        /// receive notice of cancellation.</param>
        /// <typeparam name="T">The return type.</typeparam>
        ///
        /// <exception cref="TransactionAbortedException">Thrown if the Executor lambda calls
        /// <see cref="AsyncTransactionExecutor.Abort"/>.</exception>
        /// <exception cref="QldbDriverException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        ///
        /// <returns>The Task containing return value of executing the executor. Note that if you directly return
        /// a <see cref="IAsyncResult"/>, this will be automatically buffered in memory before the implicit commit
        /// to allow reading, as the commit will close any open results. Any other <see cref="IAsyncResult"/> instances
        /// created within the executor block will be invalidated, including if the return value is an object which nests
        /// said <see cref="IAsyncResult"/> instances within it.</returns>
        Task<T> Execute<T>(Func<AsyncTransactionExecutor, Task<T>> func, CancellationToken cancellationToken = default);

        /// <summary>
        /// Execute the Async Executor lambda against QLDB and retrieve the result within a transaction.
        /// </summary>
        ///
        /// <param name="func">The Async Executor lambda representing the block of code to be executed within the
        /// transaction. This cannot have any side effects as it may be invoked multiple times, and the result cannot
        /// be trusted until the transaction is committed.</param>
        /// <param name="retryPolicy">A <see cref="RetryPolicy"/> that overrides the RetryPolicy set when creating the
        /// driver. The given retry policy will be used when retrying the transaction.</param>
        /// <param name="cancellationToken"> A cancellation token that can be used by other objects or threads to
        /// receive notice of cancellation.</param>
        /// <typeparam name="T">The return type.</typeparam>
        ///
        /// <exception cref="TransactionAbortedException">Thrown if the Executor lambda calls
        /// <see cref="AsyncTransactionExecutor.Abort"/>.</exception>
        /// <exception cref="QldbDriverException">Thrown when called on a disposed instance.</exception>
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        ///
        /// <returns>The Task containing return value of executing the executor. Note that if you directly return
        /// a <see cref="IAsyncResult"/>, this will be automatically buffered in memory before the implicit commit
        /// to allow reading, as the commit will close any open results. Any other <see cref="IAsyncResult"/> instances
        /// created within the executor block will be invalidated, including if the return value is an object which nests
        /// said <see cref="IAsyncResult"/> instances within it.</returns>
        Task<T> Execute<T>(
            Func<AsyncTransactionExecutor, Task<T>> func,
            RetryPolicy retryPolicy,
            CancellationToken cancellationToken = default);
    }
}
