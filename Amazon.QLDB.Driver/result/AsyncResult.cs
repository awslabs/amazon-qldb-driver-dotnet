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
    using System.Collections;
    using Amazon.QLDBSession.Model;

    /// <summary>
    /// Result implementation which streams data from QLDB, discarding chunks as they are read.
    ///
    /// Note that due to the fact that a result can only be retrieved from QLDB once, the Result may only be iterated
    /// over once. Attempts to do so multiple times will result in an exception.
    ///
    /// This implementation should be used by default to avoid excess memory consumption and to improve performance.
    /// </summary>
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

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }
}
