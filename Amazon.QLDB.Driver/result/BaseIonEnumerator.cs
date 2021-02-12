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
    using Amazon.IonDotnet.Builders;
    using Amazon.IonDotnet.Tree;
    using Amazon.QLDBSession.Model;

    /// <summary>
    /// Abstract class for an object which allows for iteration over the individual Ion values that make up the whole result of a statement
    /// execution against QLDB.
    /// </summary>
    internal abstract class BaseIonEnumerator
    {
        private protected readonly Session session;
        private protected readonly string txnId;
        private protected IEnumerator<ValueHolder> currentEnumerator;
        private protected string nextPageToken;
        private protected long? readIOs = null;
        private protected long? writeIOs = null;
        private protected long? processingTimeMilliseconds = null;

        private static readonly IonLoader IonLoader = IonLoader.Default;

        /// <summary>
        /// Initializes a new instance of the <see cref="BaseIonEnumerator"/> class.
        /// </summary>
        ///
        /// <param name="session">The parent session that represents the communication channel to QLDB.</param>
        /// <param name="txnId">The unique ID of the transaction.</param>
        /// <param name="statementResult">The result of the statement execution.</param>
        internal BaseIonEnumerator(Session session, string txnId, ExecuteStatementResult statementResult)
        {
            this.session = session;
            this.txnId = txnId;
            this.currentEnumerator = statementResult.FirstPage.Values.GetEnumerator();
            this.nextPageToken = statementResult.FirstPage.NextPageToken;

            if (statementResult.ConsumedIOs != null)
            {
                this.readIOs = statementResult.ConsumedIOs.ReadIOs;
                this.writeIOs = statementResult.ConsumedIOs.WriteIOs;
            }

            if (statementResult.TimingInformation != null)
            {
                this.processingTimeMilliseconds = statementResult.TimingInformation.ProcessingTimeMilliseconds;
            }
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

        /// <summary>
        /// Reset. Not supported.
        /// </summary>
        public void Reset()
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Gets the current query statistics for the number of read IO requests.
        /// The statistics are stateful.
        /// </summary>
        ///
        /// <returns>The current IOUsage statistics.</returns>
        internal IOUsage? GetConsumedIOs()
        {
            if (this.readIOs != null || this.writeIOs != null)
            {
                return new IOUsage(this.readIOs.GetValueOrDefault(), this.writeIOs.GetValueOrDefault());
            }

            return null;
        }

        /// <summary>
        /// Gets the current query statistics for server-side processing time. The statistics are stateful.
        /// </summary>
        ///
        /// <returns>The current TimingInformation statistics.</returns>
        internal TimingInformation? GetTimingInformation()
        {
            if (this.processingTimeMilliseconds == null)
            {
                return null;
            }

            return new TimingInformation(this.processingTimeMilliseconds.Value);
        }

        /// <summary>
        /// Update the metrics.
        /// </summary>
        ///
        /// <param name="pageResult">The result of a fetch page command.</param>
        private protected void UpdateMetrics(FetchPageResult pageResult)
        {
            if (pageResult.ConsumedIOs != null)
            {
                this.readIOs = this.readIOs == null ?
                    pageResult.ConsumedIOs.ReadIOs : this.readIOs + pageResult.ConsumedIOs.ReadIOs;
                this.writeIOs = this.writeIOs == null ?
                    pageResult.ConsumedIOs.WriteIOs : this.writeIOs + pageResult.ConsumedIOs.WriteIOs;
            }

            if (pageResult.TimingInformation != null)
            {
                this.processingTimeMilliseconds = this.processingTimeMilliseconds == null ?
                    pageResult.TimingInformation.ProcessingTimeMilliseconds :
                    this.processingTimeMilliseconds + pageResult.TimingInformation.ProcessingTimeMilliseconds;
            }
        }
    }
}
