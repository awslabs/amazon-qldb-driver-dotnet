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

    public abstract class BaseQldbDriverBuilder<TBuilder>
        where TBuilder : BaseQldbDriverBuilder<TBuilder>
    {
        private protected const string Version = "1.2.0";
        private protected AmazonQLDBSessionClient sessionClient;
        private protected int maxConcurrentTransactions = 0;
        private protected bool logRetries = false;
        private protected ISerializer serializer = null;

        private protected abstract TBuilder BuilderInstance { get; }

        private protected virtual string UserAgentString { get; } = "QLDB Driver for .NET v" + Version;

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

        /// <summary>
        /// Specify the credentials that should be used for the driver's sessions.
        /// </summary>
        ///
        /// <param name="credentials">The credentials to create a driver with.</param>
        ///
        /// <returns>This builder object.</returns>
        public TBuilder WithAWSCredentials(AWSCredentials credentials)
        {
            this.Credentials = credentials;
            return this.BuilderInstance;
        }

        /// <summary>
        /// Specify the ledger that should be used for the driver's sessions.
        /// </summary>
        ///
        /// <param name="ledgerName">The name of the ledger to create a driver with.</param>
        ///
        /// <returns>This builder object.</returns>
        public TBuilder WithLedger(string ledgerName)
        {
            ValidationUtils.AssertStringNotEmpty(ledgerName, "ledgerName");
            this.LedgerName = ledgerName;
            return this.BuilderInstance;
        }

        /// <summary>
        /// Specify the logger that should be used for the driver's sessions.
        /// </summary>
        ///
        /// <param name="logger">The logger to create a driver with.</param>
        ///
        /// <returns>This builder object.</returns>
        public TBuilder WithLogger(ILogger logger)
        {
            ValidationUtils.AssertNotNull(logger, "logger");
            this.Logger = logger;
            return this.BuilderInstance;
        }

        /// <summary>
        /// Specify the configuration that should be used for the driver's sessions.
        /// </summary>
        ///
        /// <param name="sessionConfig">The configuration to create a driver with.</param>
        ///
        /// <returns>This builder object.</returns>
        public TBuilder WithQLDBSessionConfig(AmazonQLDBSessionConfig sessionConfig)
        {
            this.SessionConfig = sessionConfig;
            return this.BuilderInstance;
        }

        /// <summary>
        /// <para>Specify the maximum number of concurrent transactions the driver can handle.</para>
        ///
        /// <para>Set to 0 by default to use the maximum possible amount allowed by the client
        /// builder's configuration.</para>
        /// </summary>
        ///
        /// <param name="maxConcurrentTransactions">
        /// The maximum number of transactions can be running at any one time. This amount
        /// cannot exceed the amount set in the <see cref="Amazon.Runtime.ClientConfig"/> used for this builder.
        /// </param>
        /// <returns>This builder object.</returns>
        public TBuilder WithMaxConcurrentTransactions(int maxConcurrentTransactions)
        {
            ValidationUtils.AssertNotNegative(maxConcurrentTransactions, "maxConcurrentTransactions");
            this.maxConcurrentTransactions = maxConcurrentTransactions;
            return this.BuilderInstance;
        }

        /// <summary>
        /// Enable logging driver retries at the WARN level.
        /// </summary>
        /// <returns>This builder object.</returns>
        public TBuilder WithRetryLogging()
        {
            this.logRetries = true;
            return this.BuilderInstance;
        }

        /// <summary>
        /// Specify the serializer that should be used to serialize and deserialize Ion data.
        /// </summary>
        ///
        /// <param name="serializer">The serializer to create the driver with.</param>
        ///
        /// <returns>This builder object.</returns>
        public TBuilder WithSerializer(ISerializer serializer)
        {
            this.serializer = serializer;
            return this.BuilderInstance;
        }

        /// <summary>
        /// Set defaults and verify the current configuration set with the builder.
        /// </summary>
        private protected void PrepareBuild()
        {
            this.SessionConfig ??= new AmazonQLDBSessionConfig();

            // Set SDK retry to 0 in order to let driver handle retry logic.
            this.SessionConfig.MaxErrorRetry = 0;
            this.sessionClient = this.Credentials == null ? new AmazonQLDBSessionClient(this.SessionConfig)
                : new AmazonQLDBSessionClient(this.Credentials, this.SessionConfig);
            this.sessionClient.BeforeRequestEvent += this.SetUserAgent;

            ValidationUtils.AssertStringNotEmpty(this.LedgerName, "ledgerName");

            if (this.maxConcurrentTransactions == 0)
            {
                this.maxConcurrentTransactions = this.SessionConfig.GetType().GetProperty("MaxConnectionsPerServer") ==
                    null ? int.MaxValue : this.GetMaxConn();
            }
        }

        private protected void SetUserAgent(object sender, RequestEventArgs eventArgs)
        {
            const string UserAgentHeader = "User-Agent";

            if (eventArgs is not WebServiceRequestEventArgs args || !args.Headers.ContainsKey(UserAgentHeader))
            {
                return;
            }

            if (!args.Headers[UserAgentHeader].Contains(this.UserAgentString))
            {
                args.Headers[UserAgentHeader] = args.Headers[UserAgentHeader] + this.UserAgentString;
            }
        }

        /// <summary>
        /// We need this to bypass a bug in .Net Framework.
        /// </summary>
        /// <returns>Max connections per server from the AWS SDK config.</returns>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private protected int GetMaxConn()
        {
            return this.SessionConfig.MaxConnectionsPerServer ?? int.MaxValue;
        }
    }
}
