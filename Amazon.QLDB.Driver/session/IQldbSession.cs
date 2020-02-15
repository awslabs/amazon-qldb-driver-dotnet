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
    /// <para>There are three methods of execution, ranging from simple to complex; the first two are recommended for their inbuilt
    /// error handling:
    /// <list type="bullet">
    /// <item><description>Execute(string, Action), Execute(string, Action, List), and Execute(string, Action, params IIonValue[])
    /// allow for a single statement to be executed within a transaction where the transaction is implicitly created
    /// and committed, and any recoverable errors are transparently handled.</description></item>
    /// <item><description>Execute(Action, Action) and Execute(Func, Action) allow for more complex execution sequences where
    /// more than one execution can occur, as well as other method calls. The transaction is implicitly created and committed, and any
    /// recoverable errors are transparently handled.</description></item>
    /// <item><description><see cref="StartTransaction"/> allows for full control over when the transaction is committed and leaves the
    /// responsibility of OCC conflict handling up to the user. Transaction methods cannot be automatically retried, as
    /// the state of the transaction is ambiguous in the case of an unexpected error.</description></item>
    /// </list>
    /// </para>
    /// </summary>
    public interface IQldbSession : IDisposable, IRetriableExecutable
    {
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
