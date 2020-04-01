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
    using Amazon.QLDBSession;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Abstract base class for QldbDriver objects.
    /// </summary>
    public abstract class BaseQldbDriver : IQldbDriver
    {
#pragma warning disable SA1600 // Elements should be documented
        private protected readonly string ledgerName;
        private protected readonly AmazonQLDBSessionClient sessionClient;
        private protected readonly int retryLimit;
        private protected readonly ILogger logger;
        private protected bool isClosed = false;
#pragma warning restore SA1600 // Elements should be documented

        /// <summary>
        /// Initializes a new instance of the <see cref="BaseQldbDriver"/> class.
        /// </summary>
        ///
        /// <param name="ledgerName">The ledger to create sessions to.</param>
        /// <param name="sessionClient">QLDB session client.</param>
        /// <param name="retryLimit">The amount of retries sessions created by this driver will attempt upon encountering a non-fatal error.</param>
        /// <param name="logger">Logger to be used by this.</param>
        internal BaseQldbDriver(string ledgerName, AmazonQLDBSessionClient sessionClient, int retryLimit, ILogger logger)
        {
            this.ledgerName = ledgerName;
            this.sessionClient = sessionClient;
            this.retryLimit = retryLimit;
            this.logger = logger;
        }

        /// <summary>
        /// Close this driver. No-op if already closed.
        /// </summary>
        public abstract void Dispose();

        /// <summary>
        /// <para>Get a <see cref="IQldbSession"/> object.</para>
        /// </summary>
        ///
        /// <returns>The <see cref="IQldbSession"/> object.</returns>
        public abstract IQldbSession GetSession();
    }
}
