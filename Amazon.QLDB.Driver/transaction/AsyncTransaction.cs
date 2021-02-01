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
    using System.Threading.Tasks;
    using Amazon.IonDotnet.Tree;
    using Microsoft.Extensions.Logging;

    internal class AsyncTransaction : BaseTransaction, IAsyncTransaction
    {
        private readonly Session session;
        private readonly string txnId;
        private readonly ILogger logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncTransaction"/> class.
        /// </summary>
        ///
        /// <param name="session">The parent session that represents the communication channel to QLDB.</param>
        /// <param name="txnId">Transaction identifier.</param>
        /// <param name="logger">Logger to be used by this.</param>
        internal AsyncTransaction(Session session, string txnId, ILogger logger)
        {
            this.session = session;
            this.txnId = txnId;
            this.logger = logger;
            this.qldbHash = QldbHash.ToQldbHash(txnId);
        }

        public override void Abort()
        {
            throw new NotImplementedException();
        }

        public override void Commit()
        {
            throw new NotImplementedException();
        }

        public Task<IAsyncResult> Execute(string statement)
        {
            throw new NotImplementedException();
        }

        public Task<IAsyncResult> Execute(string statement, List<IIonValue> parameters)
        {
            throw new NotImplementedException();
        }

        public Task<IAsyncResult> Execute(string statement, params IIonValue[] parameters)
        {
            throw new NotImplementedException();
        }
    }
}
