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
    using Amazon.QLDBSession;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Builder object for creating a <see cref="AsyncQldbDriver"/>, allowing for configuration of the parameters of
    /// construction.
    /// </summary>
    public class AsyncQldbDriverBuilder : BaseQldbDriverBuilder
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncQldbDriverBuilder"/> class.
        /// </summary>
        internal AsyncQldbDriverBuilder()
        {
        }

        /// <summary>
        /// Specify the credentials that should be used for the driver's sessions.
        /// </summary>
        ///
        /// <param name="credentials">The credentials to create a driver with.</param>
        ///
        /// <returns>This builder object.</returns>
        public AsyncQldbDriverBuilder WithAWSCredentials(AWSCredentials credentials)
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
        public AsyncQldbDriverBuilder WithLedger(string ledgerName)
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
        public AsyncQldbDriverBuilder WithLogger(ILogger logger)
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
        public AsyncQldbDriverBuilder WithQLDBSessionConfig(AmazonQLDBSessionConfig sessionConfig)
        {
            this.SessionConfig = sessionConfig;
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
        public AsyncQldbDriverBuilder WithMaxConcurrentTransactions(int maxConcurrentTransactions)
        {
            ValidationUtils.AssertNotNegative(maxConcurrentTransactions, "maxConcurrentTransactions");
            this.maxConcurrentTransactions = maxConcurrentTransactions;
            return this;
        }

        /// <summary>
        /// Enable loggging driver retries at the WARN level.
        /// </summary>
        /// <returns>This builder object.</returns>
        public AsyncQldbDriverBuilder WithRetryLogging()
        {
            this.logRetries = true;
            return this;
        }

        /// <summary>
        /// Build a driver instance using the current configuration set with the builder.
        /// </summary>
        ///
        /// <returns>A newly created driver.</returns>
        public AsyncQldbDriver Build()
        {
            this.PrepareForBuild();

            return new AsyncQldbDriver(
                new AsyncSessionPool(
                    () => Session.StartSession(this.LedgerName, this.sessionClient, this.Logger),
                    CreateDefaultRetryHandler(this.logRetries ? this.Logger : null),
                    this.maxConcurrentTransactions,
                    this.Logger));
        }

        /// <summary>
        /// Create a AsyncRetryHandler object with the default set of retriable exceptions.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <returns>The constructed IAsyncRetryHandler instance.</returns>
        internal static IAsyncRetryHandler CreateDefaultRetryHandler(ILogger logger)
        {
            return new AsyncRetryHandler(logger);
        }
    }
}
