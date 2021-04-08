﻿/*
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
    using System.IO;
    using System.Collections;
    using System.Collections.Generic;
    using Amazon.IonDotnet.Tree;
    using Amazon.QLDBSession.Model;

    /// <summary>
    /// Result implementation which streams data from QLDB, discarding chunks as they are read.
    ///
    /// Note that due to the fact that a result can only be retrieved from QLDB once, the Result may only be iterated
    /// over once. Attempts to do so multiple times will result in an exception.
    ///
    /// This implementation should be used by default to avoid excess memory consumption and to improve performance.
    /// </summary>
    internal class Result : IResult
    {
        private readonly IonEnumerator ionEnumerator;
        private bool isRetrieved = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="Result"/> class.
        /// </summary>
        ///
        /// <param name="session">The parent session that represents the communication channel to QLDB.</param>
        /// <param name="txnId">The ID of the parent transaction.</param>
        /// <param name="statementResult">The result of the statement execution.</param>
        internal Result(Session session, string txnId, ExecuteStatementResult statementResult)
        {
            this.ionEnumerator = new IonEnumerator(session, txnId, statementResult);
        }

        /// <inheritdoc/>
        public IEnumerator<IIonValue> GetEnumerator()
        {
            if (this.isRetrieved)
            {
                throw new InvalidOperationException();
            }

            this.isRetrieved = true;
            return this.ionEnumerator;
        }

        /// <inheritdoc/>
        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
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
        /// Convert this IEnumerable to enumerate over a different value.
        /// </summary>
        public IEnumerable<R> Select<R>(Func<Stream, R> selector) 
        {
            return new MappedEnumerable<R>(this.ionEnumerator, selector);
        }

        private class MappedEnumerable<T> : IEnumerable<T>
        {
            private readonly IonEnumerator ionEnumerator;
            private readonly Func<Stream, T> selector;

            public MappedEnumerable(IonEnumerator ionEnumerator, Func<Stream, T> selector)
            {
                this.ionEnumerator = ionEnumerator;
                this.selector = selector;
            }

            public IEnumerator<T> GetEnumerator()
            {
                return new MappedEnumerator<T>(ionEnumerator, selector);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return this.GetEnumerator();
            }
        }

        private class MappedEnumerator<T> : IEnumerator<T> 
        {
            private readonly IonEnumerator ionEnumerator;
            private readonly Func<Stream, T> selector;

            public MappedEnumerator(IonEnumerator ionEnumerator, Func<Stream, T> selector)
            {
                this.ionEnumerator = ionEnumerator;
                this.selector = selector;
            }

            public T Current => selector(ionEnumerator.CurrentIonBinary);

            object IEnumerator.Current => Current;

            public void Dispose()
            {
                ionEnumerator.Dispose();
            }

            public bool MoveNext()
            {
                return ionEnumerator.MoveNext();
            }

            public void Reset()
            {
                ionEnumerator.Reset();
            }
        }

        /// <summary>
        /// Object which allows for iteration over the individual Ion values that make up the whole result of a statement
        /// execution against QLDB.
        /// </summary>
        private class IonEnumerator : BaseIonEnumerator, IEnumerator<IIonValue>
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="IonEnumerator"/> class.
            /// </summary>
            ///
            /// <param name="session">The parent session that represents the communication channel to QLDB.</param>
            /// <param name="txnId">The unique ID of the transaction.</param>
            /// <param name="statementResult">The result of the statement execution.</param>
            internal IonEnumerator(Session session, string txnId, ExecuteStatementResult statementResult)
                : base(session, txnId, statementResult)
            {
            }

            object IEnumerator.Current => this.Current;

            /// <summary>
            /// Dispose the enumerator. No-op.
            /// </summary>
            public void Dispose()
            {
                return;
            }

            /// <summary>
            /// Reset. Not supported.
            /// </summary>
            public void Reset()
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Advance the enumerator to the next value within the page.
            /// </summary>
            ///
            /// <returns>True if there is another page token.</returns>
            public bool MoveNext()
            {
                if (this.currentEnumerator.MoveNext())
                {
                    return true;
                }
                else if (this.nextPageToken == null)
                {
                    return false;
                }

                this.FetchPage();
                return this.MoveNext();
            }

            /// <summary>
            /// Fetch the next page from the session.
            /// </summary>
            private void FetchPage()
            {
                FetchPageResult pageResult = this.session.FetchPage(this.txnId, this.nextPageToken);
                this.currentEnumerator = pageResult.Page.Values.GetEnumerator();
                this.nextPageToken = pageResult.Page.NextPageToken;
                this.UpdateMetrics(pageResult);
            }
        }
    }
}
