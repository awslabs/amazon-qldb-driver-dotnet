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
    using System.Runtime.CompilerServices;
    using Amazon.QLDBSession;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging;
    using Microsoft.Extensions.Logging.Abstractions;

    /// <summary>
    /// Builder object for creating a <see cref="QldbDriver"/>, allowing for configuration of the parameters of
    /// construction.
    /// </summary>
    public class QldbDriverBuilder
    {
        private protected AmazonQLDBSessionClient sessionClient;
        private const int DefaultRetryLimit = 4;

        private int maxConcurrentTransactions = 0;

        /// <summary>
        /// Initializes a new instance of the <see cref="QldbDriverBuilder"/> class.
        /// </summary>
        internal QldbDriverBuilder()
        {
        }

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
        /// Gets or sets the number of retry attempts to be made by the session.
        /// </summary>
        private protected int RetryLimit { get; set; } = DefaultRetryLimit;

        /// <summary>
        /// Specify the credentials that should be used for the driver's sessions.
        /// </summary>
        ///
        /// <param name="credentials">The credentials to create a driver with.</param>
        ///
        /// <returns>This builder object.</returns>
        public QldbDriverBuilder WithAWSCredentials(AWSCredentials credentials)
        {
            this.Credentials = credentials;
            return this;
        }

        /// <summary>
        /// Specify the ledger that should be used for the driver's sessions.
        /// </summary>
        ///
        /// <param name="ledgerName">The name of the ledger to create a driver with.</param>
        ///
        /// <returns>This builder object.</returns>
        public QldbDriverBuilder WithLedger(string ledgerName)
        {
            ValidationUtils.AssertStringNotEmpty(ledgerName, "ledgerName");
            this.LedgerName = ledgerName;
            return this;
        }

        /// <summary>
        /// Specify the logger that should be used for the driver's sessions.
        /// </summary>
        ///
        /// <param name="logger">The logger to create a driver with.</param>
        ///
        /// <returns>This builder object.</returns>
        public QldbDriverBuilder WithLogger(ILogger logger)
        {
            ValidationUtils.AssertNotNull(logger, "logger");
            this.Logger = logger;
            return this;
        }

        /// <summary>
        /// Specify the configuration that should be used for the driver's sessions.
        /// </summary>
        ///
        /// <param name="sessionConfig">The configuration to create a driver with.</param>
        ///
        /// <returns>This builder object.</returns>
        public QldbDriverBuilder WithQLDBSessionConfig(AmazonQLDBSessionConfig sessionConfig)
        {
            this.SessionConfig = sessionConfig;
            return this;
        }

        /// <summary>
        /// Specify the retry limit that any convenience execute methods provided by sessions created from the driver
        /// will attempt.
        /// </summary>
        ///
        /// <param name="retryLimit">The number of retry attempts to be made by the session.</param>
        ///
        /// <returns>This builder object.</returns>
        public QldbDriverBuilder WithRetryLimit(int retryLimit)
        {
            ValidationUtils.AssertNotNegative(retryLimit, "retryLimit");
            this.RetryLimit = retryLimit;
            return this;
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
        public QldbDriverBuilder WithMaxConcurrentTransactions(int maxConcurrentTransactions)
        {
            ValidationUtils.AssertNotNegative(maxConcurrentTransactions, "maxConcurrentTransactions");
            this.maxConcurrentTransactions = maxConcurrentTransactions;
            return this;
        }

        /// <summary>
        /// Build a driver instance using the current configuration set with the builder.
        /// </summary>
        ///
        /// <returns>A newly created driver.</returns>
        public QldbDriver Build()
        {
            if (this.SessionConfig == null)
            {
                this.SessionConfig = new AmazonQLDBSessionConfig();
            }

            this.SessionConfig.MaxErrorRetry = 0;
            this.sessionClient = this.Credentials == null ? new AmazonQLDBSessionClient(this.SessionConfig)
                : new AmazonQLDBSessionClient(this.Credentials, this.SessionConfig);
            this.sessionClient.BeforeRequestEvent += SetUserAgent;

            ValidationUtils.AssertStringNotEmpty(this.LedgerName, "ledgerName");

            if (this.maxConcurrentTransactions == 0)
            {
                this.maxConcurrentTransactions = this.SessionConfig.GetType().GetProperty("MaxConnectionsPerServer") == null ?
                    int.MaxValue : this.GetMaxConn();
            }

            return new QldbDriver(
                new SessionPool(
                () => Session.StartSession(this.LedgerName, this.sessionClient, this.Logger),
                this.RetryLimit,
                this.maxConcurrentTransactions,
                this.Logger), this.Logger);
        }

        private static void SetUserAgent(object sender, RequestEventArgs eventArgs)
        {
            const string UserAgentHeader = "User-Agent";
            const string Version = "1.0.0-rc.1";

#pragma warning disable IDE0019 // Use pattern matching
            var args = eventArgs as WebServiceRequestEventArgs;
#pragma warning restore IDE0019 // Use pattern matching
            if (args == null || !args.Headers.ContainsKey(UserAgentHeader))
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
        private int GetMaxConn()
        {
            return this.SessionConfig.MaxConnectionsPerServer ?? int.MaxValue;
        }
    }
}
