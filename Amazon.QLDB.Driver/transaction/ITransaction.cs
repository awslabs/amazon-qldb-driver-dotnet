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

    /// <summary>
    /// <para>Interface that represents an active transaction with QLDB.</para>
    ///
    /// <para>Every transaction is tied to the parent <see cref="IQldbSession"/>, meaning that if the parent session is closed or
    /// invalidated, the child transaction is automatically closed and cannot be used. Only one transaction can be active at
    /// any given time per parent session, and thus every transaction should call <see cref="Abort"/> or <see cref="Commit"/> when
    /// it is no longer needed, or when a new transaction is wanted from the parent session.</para>
    ///
    /// <para>Any unexpected errors that occur within a transaction should not be retried using the same transaction, as the
    /// state of the transaction is now ambiguous.</para>
    ///
    /// <para>When an OCC conflict occurs, the transaction is closed and must be handled manually by creating a new transaction
    /// and re-executing the desired queries.</para>
    ///
    /// <para>Child Result objects will be closed when the transaction is aborted or committed.</para>
    /// </summary>
    internal interface ITransaction : IDisposable, IExecutable
    {
        string Id { get; }

        /// <summary>
        /// Abort the transaction and roll back any changes. No-op if closed.
        /// Any open <see cref="IResult"/> created by the transaction will be invalidated.
        /// </summary>
        void Abort();

        /// <summary>
        /// Commit the transaction. Any open <see cref="IResult"/> created by the transaction will be invalidated.
        /// </summary>
        ///
        /// <exception cref="InvalidOperationException">Thrown when Hash returned from QLDB is not equal.</exception>
        /// <exception cref="OccConflictException">Thrown if an OCC conflict has been detected within the transaction.</exception>
        /// <exception cref="AmazonServiceException">Thrown when there is an error committing this transaction against QLDB.</exception>
        /// <exception cref="QldbDriverException">Thrown when this transaction has been disposed.</exception>
        void Commit();
    }
}
