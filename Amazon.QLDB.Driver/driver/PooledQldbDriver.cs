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

    /// <summary>
    /// <para>Represents a factory for accessing pooled sessions to a specific ledger within QLDB. This class or
    /// <see cref="QldbDriver"/> should be the main entry points to any interaction with QLDB. <see cref="GetSession"/> will create a
    /// <see cref="PooledQldbSession"/> to the specified ledger within QLDB as a communication channel. Any acquired sessions must
    /// be cleaned up with <see cref="PooledQldbSession.Dispose"/> when they are no longer needed in order to return the session to
    /// the pool. If this is not done, this driver may become unusable if the pool limit is exceeded.</para>
    ///
    /// <para>This factory pools sessions and attempts to return unused but available sessions when getting new sessions. The
    /// advantage to using this over the non-pooling driver is that the underlying connection that sessions use to
    /// communicate with QLDB can be recycled, minimizing resource usage by preventing unnecessary connections and reducing
    /// latency by not making unnecessary requests to start new connections and end reusable, existing, ones.</para>
    ///
    /// <para>The pool does not remove stale sessions until a new session is retrieved. The default pool size is the maximum
    /// amount of connections the session client allows set in the <see cref="Amazon.Runtime.ClientConfig"/>. <see cref="Dispose"/>
    /// should be called when this factory is no longer needed in order to clean up resources, ending all sessions in the pool.</para>
    /// </summary>
    public class PooledQldbDriver : IQldbDriver
    {
        internal PooledQldbDriver(
            string ledgerName,
            AmazonQLDBSessionClient sessionClient,
            int retryLimit,
            int poolLimit,
            int timeout)
        {
        }

        /// <summary>
        /// Retrieve a builder object for creating a <see cref="PooledQldbDriver"/>.
        /// </summary>
        /// <returns>The builder object for creating a <see cref="PooledQldbDriver"/></returns>.
        public static PooledQldbDriverBuilder Builder()
        {
            throw new System.NotImplementedException();
        }

        /// <summary>
        /// Disposes of the driver.
        /// </summary>
        public void Dispose()
        {
            throw new System.NotImplementedException();
        }

        /// <summary>
        /// <para>Get a <see cref="QldbSession"/> object.</para>
        ///
        /// <para>This will attempt to retrieve an active existing session, or it will start a new session with QLDB unless the
        /// number of allocated sessions has exceeded the pool size limit. If so, then it will continue trying to retrieve an
        /// active existing session until the timeout is reached, throwing a <see cref="AmazonQLDBSessionException"/>.</para>
        /// </summary>
        /// <returns>The <see cref="IQldbSession"/> object.</returns>
        public IQldbSession GetSession()
        {
            throw new System.NotImplementedException();
        }

        /// <summary>
        /// Builder object for creating a <see cref="PooledQldbSession"/>, allowing for configuration of the parameters of
        /// construction.
        /// </summary>
        public class PooledQldbDriverBuilder : BaseQldbDriverBuilder<PooledQldbDriverBuilder, PooledQldbDriver>
        {
            /// <summary>
            /// Restricted constructor. Use <see cref="Builder"/> to retrieve an instance of this class.
            /// </summary>
            internal PooledQldbDriverBuilder()
                : base()
            {
            }

            /// <summary>
            /// <para>Specify the limit to the pool of available sessions.</para>
            ///
            /// <para>Attempting to retrieve a session when the maximum number of sessions is already withdrawn will block until
            /// a session becomes available. Set to 0 by default to use the maximum possible amount allowed by the client
            /// builder's configuration.</para>
            /// </summary>
            ///
            /// <param name="poolLimit">
            /// The maximum number of sessions that can be created from the pool at any one time. This amount
            /// cannot exceed the amount set in the <see cref="Amazon.Runtime.ClientConfig"/> used for this builder.
            /// </param>
            /// <returns>This builder object.</returns>
            public PooledQldbDriverBuilder WithPoolLimit(int poolLimit)
            {
                throw new System.NotImplementedException();
            }

            /// <summary>
            /// <para>Specify the timeout to wait for an available session to return to the pool in milliseconds.</para>
            ///
            /// <para>Calling <see cref="GetSession"/> will wait until the timeout before throwing an exception if an available
            /// session is still not returned to the pool.</para>
            /// </summary>
            ///
            /// <param name="timeout">The maximum amount of time to wait, in milliseconds.</param>
            ///
            /// <returns>This builder object.</returns>
            public PooledQldbDriverBuilder WithTimeout(int timeout)
            {
                throw new System.NotImplementedException();
            }

            /// <summary>
            /// Creates the pooled driver.
            /// </summary>
            internal override PooledQldbDriver ConstructDriver()
            {
                throw new System.NotImplementedException();
            }
        }
    }
}
