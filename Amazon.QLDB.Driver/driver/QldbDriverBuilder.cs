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
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Builder object for creating a <see cref="QldbDriver"/>, allowing for configuration of the parameters of
    /// construction.
    /// </summary>
    public class QldbDriverBuilder : BaseQldbDriverBuilder<QldbDriverBuilder>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="QldbDriverBuilder"/> class.
        /// </summary>
        internal QldbDriverBuilder()
        {
        }

        private protected override QldbDriverBuilder BuilderInstance => this;

        private protected override string UserAgentStringPrefix => " QLDBDriver for .NET v";

        /// <summary>
        /// Build a driver instance using the current configuration set with the builder.
        /// </summary>
        ///
        /// <returns>A newly created driver.</returns>
        public QldbDriver Build()
        {
            this.PrepareBuild();
            return new QldbDriver(
                new SessionPool(
                    () => Session.StartSession(this.LedgerName, this.sessionClient, this.Logger),
                    CreateDefaultRetryHandler(this.logRetries ? this.Logger : null),
                    this.maxConcurrentTransactions,
                    this.Logger));
        }

        /// <summary>
        /// Create a RetryHandler object with the default set of retriable exceptions.
        /// </summary>
        /// <param name="logger">The logger.</param>
        /// <returns>The constructed IRetryHandler instance.</returns>
        internal static IRetryHandler CreateDefaultRetryHandler(ILogger logger)
        {
            return new RetryHandler(logger);
        }
    }
}
