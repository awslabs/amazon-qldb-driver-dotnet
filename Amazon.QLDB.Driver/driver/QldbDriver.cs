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
    using System;
    using Amazon.QLDBSession;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// <para>Represents a factory for accessing sessions to a specific ledger within QLDB. This class or
    /// <see cref="PooledQldbDriver"/> should be the main entry points to any interaction with QLDB. <see cref="GetSession"/> will
    /// create a <see cref="QldbSession"/> to the specified ledger within QLDB as a communication channel. Any sessions acquired
    /// should be cleaned up with <see cref="QldbSession.Dispose"/> to free up resources.</para>
    ///
    /// <para>This factory does not attempt to re-use or manage sessions in any way. It is recommended to use
    /// <see cref="PooledQldbDriver"/> for both less resource usage and lower latency.</para>
    /// </summary>
    public class QldbDriver : IQldbDriver
    {
        private readonly string ledgerName;
        private readonly AmazonQLDBSessionClient sessionClient;
        private readonly int retryLimit;
        private readonly ILogger logger;
        private bool isClosed = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="QldbDriver"/> class.
        /// </summary>
        /// <param name="ledgerName">The ledger to create sessions to.</param>
        /// <param name="sessionClient">QLDB session client.</param>
        /// <param name="retryLimit">The amount of retries sessions created by this driver will attempt upon encountering a non-fatal error.</param>
        /// <param name="logger">Logger to be used by this.</param>
        internal QldbDriver(string ledgerName, AmazonQLDBSessionClient sessionClient, int retryLimit, ILogger logger)
        {
            this.ledgerName = ledgerName;
            this.sessionClient = sessionClient;
            this.retryLimit = retryLimit;
            this.logger = logger;
        }

        /// <summary>
        /// Retrieve a builder object for creating a <see cref="QldbDriver"/>.
        /// </summary>
        ///
        /// <returns>The builder object for creating a <see cref="QldbDriver"/>.</returns>
        public static QldbDriverBuilder Builder()
        {
            return new QldbDriverBuilder();
        }

        /// <summary>
        /// Close the session.
        /// </summary>
        public void Dispose()
        {
            this.isClosed = true;
        }

        /// <summary>
        /// <para>Create and return a newly instantiated <see cref="IQldbSession"/> object.</para>
        ///
        /// <para>This will implicitly start a new session with QLDB.</para>
        /// </summary>
        ///
        /// <returns>The newly active <see cref="IQldbSession"/> object.</returns>
        ///
        /// <exception cref="ObjectDisposedException">Thrown when this driver has been disposed.</exception>
        public IQldbSession GetSession()
        {
            if (this.isClosed)
            {
                this.logger.LogError(ExceptionMessages.DriverClosed);
                throw new ObjectDisposedException(ExceptionMessages.DriverClosed);
            }

            this.logger.LogDebug("Creating new session.");
            var session = Session.StartSession(this.ledgerName, this.sessionClient, this.logger);
            return new QldbSession(session, this.retryLimit, this.logger);
        }

        /// <summary>
        /// Builder object for creating a <see cref="QldbDriver"/>, allowing for configuration of the parameters of
        /// construction.
        /// </summary>
        public class QldbDriverBuilder : BaseQldbDriverBuilder<QldbDriverBuilder, QldbDriver>
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="QldbDriverBuilder"/> class.
            /// </summary>
            internal QldbDriverBuilder()
                : base()
            {
            }

            /// <inheritdoc/>
            internal override QldbDriver ConstructDriver()
            {
                return new QldbDriver(this.LedgerName, this.sessionClient, this.RetryLimit, this.Logger);
            }
        }
    }
}
