﻿/*
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
    using System.Collections;
    using System.Collections.Generic;
    using Amazon.IonDotnet.Builders;
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
        private readonly IEnumerator<IIonValue> ionEnumerator;
        private bool isRetrieved = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="Result"/> class.
        /// </summary>
        ///
        /// <param name="session">The parent session that represents the communication channel to QLDB.</param>
        /// <param name="firstPage">The first chunk of the result, returned by the initial execution.</param>
        /// <param name="txnId">The unique ID of the transaction.</param>
        internal Result(Session session, string txnId, Page firstPage)
        {
            this.ionEnumerator = new IonEnumerator(session, txnId, firstPage);
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
        /// Object which allows for iteration over the individual Ion values that make up the whole result of a statement
        /// execution against QLDB.
        /// </summary>
        private class IonEnumerator : IEnumerator<IIonValue>
        {
            private static readonly IonLoader IonLoader = IonLoader.Default;

            private readonly Session session;
            private readonly string txnId;
            private IEnumerator<ValueHolder> currentEnumerator;
            private string nextPageToken;

            /// <summary>
            /// Initializes a new instance of the <see cref="IonEnumerator"/> class.
            /// </summary>
            ///
            /// <param name="session">The parent session that represents the communication channel to QLDB.</param>
            /// <param name="txnId">The unique ID of the transaction.</param>
            /// <param name="firstPage">The first chunk of the result, returned by the initial execution.</param>
            internal IonEnumerator(Session session, string txnId, Page firstPage)
            {
                this.session = session;
                this.txnId = txnId;
                this.currentEnumerator = firstPage.Values.GetEnumerator();
                this.nextPageToken = firstPage.NextPageToken;
            }

            /// <summary>
            /// Gets current IIonValue.
            /// </summary>
            ///
            /// <returns>The current IIonValue.</returns>
            public IIonValue Current
            {
                get
                {
                    return IonLoader.Load(this.currentEnumerator.Current.IonBinary).GetElementAt(0);
                }
            }

            object IEnumerator.Current => this.Current;

            /// <summary>
            /// Clean up.
            /// </summary>
            public void Dispose()
            {
                return;
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
            /// Reset. Not supported.
            /// </summary>
            public void Reset()
            {
                throw new NotSupportedException();
            }

            /// <summary>
            /// Fetch the next page from the session.
            /// </summary>
            private void FetchPage()
            {
                Page newPage = this.session.FetchPage(this.txnId, this.nextPageToken).Page;
                this.currentEnumerator = newPage.Values.GetEnumerator();
                this.nextPageToken = newPage.NextPageToken;
            }
        }
    }
}
