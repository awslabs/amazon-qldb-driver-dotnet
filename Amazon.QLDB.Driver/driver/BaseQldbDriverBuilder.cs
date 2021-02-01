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
    using System.Runtime.CompilerServices;
    using Amazon.QLDBSession;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Abstractions;

    /// <summary>
    /// Base class for builder object for creating a QLDB Driver, allowing for configuration of the parameters of
    /// construction.
    /// </summary>
    public abstract class BaseQldbDriverBuilder
    {
        private protected AmazonQLDBSessionClient sessionClient;

        protected int maxConcurrentTransactions = 0;
        protected bool logRetries = false;

        /// <summary>
        /// Gets or sets the AWS credentials to construct the <see cref="AmazonQLDBSessionClient"/> object.
        /// </summary>
        private protected AWSCredentials Credentials { get; set; } = null;

        /// <summary>
        /// Gets or sets the ledger that should be used for the driver's sessions.
        /// </summary>
        private protected string LedgerName { get; set; } = null;

        /// <summary>
        /// Gets or sets the logger to create a driver with.
        /// </summary>
        private protected ILogger Logger { get; set; } = NullLogger.Instance;

        /// <summary>
        /// Gets or sets the configuration to construct the <see cref="AmazonQLDBSessionClient"/> object.
        /// </summary>
        private protected AmazonQLDBSessionConfig SessionConfig { get; set; } = null;

        protected static void SetUserAgent(object sender, RequestEventArgs eventArgs)
        {
            const string UserAgentHeader = "User-Agent";
            const string Version = "1.1.0";

            if (!(eventArgs is WebServiceRequestEventArgs args) || !args.Headers.ContainsKey(UserAgentHeader))
            {
                return;
            }

            var metric = " QLDB Driver for .NET v" + Version;
            if (!args.Headers[UserAgentHeader].Contains(metric))
            {
                args.Headers[UserAgentHeader] = args.Headers[UserAgentHeader] + metric;
            }
        }

        /// <summary>
        /// We need this to bypass a bug in .Net Framework.
        /// </summary>
        /// <returns>Max connections per server from the AWS SDK config.</returns>
        [MethodImpl(MethodImplOptions.NoInlining)]
        protected int GetMaxConn()
        {
            return this.SessionConfig.MaxConnectionsPerServer ?? int.MaxValue;
        }
    }
}
