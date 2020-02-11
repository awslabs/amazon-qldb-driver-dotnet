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
    /// <para>The top-level interface for a QldbSession object for interacting with QLDB. A QldbSession is linked to the
    /// specified ledger in the parent driver of the instance of the QldbSession. In any given QldbSession, only one
    /// transaction can be active at a time. This object can have only one underlying session to QLDB, and therefore the
    /// lifespan of a QldbSession is tied to the underlying session, which is not indefinite, and on expiry this QldbSession
    /// will become invalid, and a new QldbSession needs to be created from the parent driver in order to continue usage.</para>
    ///
    /// <para>When a QldbSession is no longer needed, <see cref="Dispose"/> should be invoked in order to clean up any resources.</para>
    ///
    /// <para>See <see cref="PooledQldbDriver"/> for an example of session lifecycle management, allowing the re-use of sessions when
    /// possible. There should only be one thread interacting with a session at any given time.</para>
    ///
    /// <para>There are three methods of execution, ranging from simple to complex; the first two are recommended for inbuilt
    /// error handling:
    /// <list type="bullet">
    /// <item><description><see cref="Execute(String)"/> and <see cref="Execute(String, List)"/> allow for a single statement to be executed within a
    /// transaction where the transaction is implicitly created and committed, and any recoverable errors are
    /// transparently handled.</description></item>
    /// <item><description><see cref="Execute(Executor, RetryIndicator)"/> and <see cref="Execute(ExecutorNoReturn, RetryIndicator)"/> allow for
    /// more complex execution sequences where more than one execution can occur, as well as other method calls. The
    /// transaction is implicitly created and committed, and any recoverable errors are transparently handled.</description></item>
    /// <item><description><see cref="Execute(Executor)"/> and <see cref="Execute(ExecutorNoReturn)"/> are also available, providing the same
    /// functionality as the former two functions, but without a lambda function to be invoked upon a retryable
    /// error.</item></description>
    /// <item><description><see cref="StartTransaction"/> allows for full control over when the transaction is committed and leaves the
    /// responsibility of OCC conflict handling up to the user. Transactions methods cannot be automatically retried, as
    /// the state of the transaction is ambiguous in the case of an unexpected error.</description></item>
    /// </list>
    /// </para>
    /// </summary>
    internal class PooledQldbSession : IQldbSession
    {
        internal PooledQldbSession(QldbSession qldbSession, Action<QldbSession> disposeDelegate)
        {
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }

        public IResult Execute(string statement, List<IIonValue> parameters = null, Action<int> retryAction = null)
        {
            throw new NotImplementedException();
        }

        public void Execute(Action<TransactionExecutor> action, Action<int> retryAction = null)
        {
            throw new NotImplementedException();
        }

        public T Execute<T>(Func<TransactionExecutor, T> func, Action<int> retryAction = null)
        {
            throw new NotImplementedException();
        }

        public IEnumerable<string> ListTableNames()
        {
            throw new NotImplementedException();
        }

        public ITransaction StartTransaction()
        {
            throw new NotImplementedException();
        }
    }
}
