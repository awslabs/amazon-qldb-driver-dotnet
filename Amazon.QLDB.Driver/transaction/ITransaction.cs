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
    using IonDotnet.Tree;

    /// <summary>
    /// <para>Interface that represents an active transaction with QLDB.</para>
    ///
    /// <para>Every transaction is tied to the parent <see cref="IQldbSession"/>, meaning that if the parent session is closed or
    /// invalidated, the child transaction is automatically closed and cannot be used. Only one transaction can be active at
    /// any given time per parent session, and thus every transaction should call <see cref="Abort"/> or <see cref="Commit"/> when
    /// it is no longer needed, or when a new transaction is wanted from the parent session.</para>
    ///
    /// <para>An <see cref="Amazon.QLDBSession.Model.InvalidSessionException"/> indicates that the parent session is
    /// dead, and a new transaction cannot be created without a new <see cref="IQldbSession"/> being created from the parent
    /// driver.</para>
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
        void Abort();

        void Commit();

        IResult Execute(string statement, List<IIonValue> parameters = null);
    }
}
