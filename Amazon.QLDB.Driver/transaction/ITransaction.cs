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
    public interface ITransaction : IDisposable
    {
        /// <summary>
        /// Aborts the transaction and roll back any changes. Any open <see cref="IResult"/> created by the transaction will be closed.
        /// </summary>
        void Abort();

        /// <summary>
        /// Commits the transaction. Any open <see cref="IResult"/> created by the transaction will be closed.
        /// </summary>
        ///
        /// <exception cref="InvalidOperationException">Hash digest not equal.</exception>
        /// <exception cref="OccConflictException">OCC conflict.</exception>
        /// <exception cref="AmazonClientException">Communication issue with QLDB.</exception>
        void Commit();

        /// <summary>
        /// Executes the statement using the specified parameters against QLDB and retrieve the result.
        /// </summary>
        ///
        /// <param name="statement">PartiQL statement.</param>
        /// <param name="parameters">Ion value parameters.</param>
        ///
        /// <returns>Result from executed statement.</returns>
        IResult Execute(string statement, List<IIonValue> parameters = null);
    }
}
