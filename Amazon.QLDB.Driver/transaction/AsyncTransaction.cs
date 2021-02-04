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

    internal class AsyncTransaction
    {
        public string Id => throw new NotImplementedException();

        public async Task Abort(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public async Task Commit(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public async ValueTask DisposeAsync()
        {
            throw new NotImplementedException();
        }

        public async Task<IAsyncResult> Execute(string statement, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public async Task<IAsyncResult> Execute(
            string statement, List<IIonValue> parameters, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public async Task<IAsyncResult> Execute(
            string statement, CancellationToken cancellationToken = default, params IIonValue[] parameters)
        {
            throw new NotImplementedException();
        }
    }
}
