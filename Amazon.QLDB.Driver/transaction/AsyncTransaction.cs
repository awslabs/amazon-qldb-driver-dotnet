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
    using Amazon.IonDotnet.Tree;
    using Microsoft.Extensions.Logging;

    internal class AsyncTransaction
    {
        internal AsyncTransaction(Session session, string txnId, ILogger logger, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        internal string Id => throw new NotImplementedException();

        internal async Task Abort()
        {
            throw new NotImplementedException();
        }

        internal async Task Commit()
        {
            throw new NotImplementedException();
        }

        internal async Task<IAsyncResult> Execute(string statement)
        {
            throw new NotImplementedException();
        }

        internal async Task<IAsyncResult> Execute(string statement, List<IIonValue> parameters)
        {
            throw new NotImplementedException();
        }

        internal async Task<IAsyncResult> Execute(string statement, params IIonValue[] parameters)
        {
            throw new NotImplementedException();
        }
    }
}
