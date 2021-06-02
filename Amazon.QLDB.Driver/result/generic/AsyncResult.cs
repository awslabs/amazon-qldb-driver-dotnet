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

namespace Amazon.QLDB.Driver.Generic
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.IonDotnet.Tree;

    /// <summary>
    /// Result implementation which asynchronously streams data from QLDB, discarding chunks as they are read.
    ///
    /// Note that due to the fact that a result can only be retrieved from QLDB once, the Result may only be iterated
    /// over once. Attempts to do so multiple times will result in an exception.
    ///
    /// This implementation should be used by default to avoid excess memory consumption and to improve performance.
    /// </summary>
    internal class AsyncResult<T> : IAsyncResult<T>
    {
        private readonly IonAsyncEnumerator ionEnumerator;
        private bool isRetrieved = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncResult"/> class.
        /// </summary>
        ///
        /// <param name="session">The parent session that represents the communication channel to QLDB.</param>
        /// <param name="txnId">The ID of the parent transaction.</param>
        /// <param name="statementResult">The result of the statement execution.</param>
        /// <param name="cancellationToken">The CancellationToken to use for this AsyncResult.</param>
        internal AsyncResult(
            Session session,
            string txnId,
            QLDBSession.Model.ExecuteStatementResult statementResult,
            CancellationToken cancellationToken,
            IQuery<T> query)
        {
            this.ionEnumerator = new IonAsyncEnumerator(session, txnId, statementResult, cancellationToken, query);
        }

        /// <inheritdoc/>
        public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            if (this.isRetrieved)
            {
                throw new InvalidOperationException();
            }

            this.isRetrieved = true;
            if (cancellationToken != default)
            {
                this.ionEnumerator.CancellationToken = cancellationToken;
            }

            return this.ionEnumerator;
        }

        /// <summary>
        /// Gets the current query statistics for the number of read IO requests. The statistics are stateful.
        /// </summary>
        ///
        /// <returns>The current IOUsage statistics.</returns>
        public IOUsage? GetConsumedIOs()
        {
            return this.ionEnumerator.GetConsumedIOs();
        }

        /// <summary>
        /// Gets the current query statistics for server-side processing time. The statistics are stateful.
        /// </summary>
        ///
        /// <returns>The current TimingInformation statistics.</returns>
        public TimingInformation? GetTimingInformation()
        {
            return this.ionEnumerator.GetTimingInformation();
        }

        /// <summary>
        /// Object which allows for asynchronous iteration over the individual Ion values that make up the whole result
        /// of a statement execution against QLDB.
        /// </summary>
        private class IonAsyncEnumerator : BaseIonEnumerator, IAsyncEnumerator<T>
        {
            internal CancellationToken CancellationToken;
            private readonly IQuery<T> query;

            /// <summary>
            /// Initializes a new instance of the <see cref="IonAsyncEnumerator"/> class.
            /// </summary>
            ///
            /// <param name="session">The parent session that represents the communication channel to QLDB.</param>
            /// <param name="txnId">The unique ID of the transaction.</param>
            /// <param name="statementResult">The result of the statement execution.</param>
            /// <param name="cancellationToken">The CancellationToken to use for this AsyncResult.</param>
            internal IonAsyncEnumerator(
                Session session,
                string txnId,
                QLDBSession.Model.ExecuteStatementResult statementResult,
                CancellationToken cancellationToken, 
                IQuery<T> query)
                : base(session, txnId, statementResult)
            {
                this.CancellationToken = cancellationToken;
                this.query = query;
            }

            new public T Current
            {
                get
                {
                    return query.Deserialize(this.currentEnumerator.Current);
                }
            }

            /// <summary>
            /// Dispose the enumerator. No-op.
            /// </summary>
            ///
            /// <returns>No object or value is returned by this method when it completes.</returns>
            public ValueTask DisposeAsync()
            {
                return default;
            }

            /// <summary>
            /// Asynchronously advance the enumerator to the next value within the page.
            /// </summary>
            /// <returns>True if there is another page token.</returns>
            public async ValueTask<bool> MoveNextAsync()
            {
                if (this.currentEnumerator.MoveNext())
                {
                    return true;
                }
                else if (this.nextPageToken == null)
                {
                    return false;
                }

                await this.FetchPage();
                return await this.MoveNextAsync();
            }

            /// <summary>
            /// Fetch the next page from the session.
            /// </summary>
            ///
            /// <returns>No object or value is returned by this method when it completes.</returns>
            private async Task FetchPage()
            {
                QLDBSession.Model.FetchPageResult pageResult =
                    await this.session.FetchPageAsync(this.txnId, this.nextPageToken, this.CancellationToken);
                this.currentEnumerator = pageResult.Page.Values.GetEnumerator();
                this.nextPageToken = pageResult.Page.NextPageToken;
                this.UpdateMetrics(pageResult);
            }
        }
    }
}
