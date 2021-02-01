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
    using Amazon.QLDBSession.Model;

    internal class AsyncResult : BaseResult, IAsyncResult
    {
        private readonly IonEnumerator ionEnumerator;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncResult"/> class.
        /// </summary>
        ///
        /// <param name="session">The parent session that represents the communication channel to QLDB.</param>
        /// <param name="statementResult">The result of the statement execution.</param>
        /// <param name="txnId">The unique ID of the transaction.</param>
        internal AsyncResult(Session session, string txnId, ExecuteStatementResult statementResult)
        {
            this.ionEnumerator = new IonEnumerator(session, txnId, statementResult);
        }

        public IAsyncEnumerator<IIonValue> GetAsyncEnumerator(CancellationToken cancellationToken = default(CancellationToken))
        {
            throw new NotImplementedException();
        }
    }
}
