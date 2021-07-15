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
    /// <summary>
    /// Builder object for creating an <see cref="AsyncQldbDriver"/>, allowing for configuration of the parameters of
    /// construction.
    /// </summary>
    public class AsyncQldbDriverBuilder : BaseQldbDriverBuilder<AsyncQldbDriverBuilder>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncQldbDriverBuilder"/> class.
        /// </summary>
        internal AsyncQldbDriverBuilder()
        {
        }

        private protected override AsyncQldbDriverBuilder BuilderInstance => this;

        private protected override string UserAgentString => base.UserAgentString + " Async";

        /// <summary>
        /// Build a driver instance using the current configuration set with the builder.
        /// </summary>
        ///
        /// <returns>A newly created driver.</returns>
        public AsyncQldbDriver Build()
        {
            this.PrepareBuild();
            return new AsyncQldbDriver(
                this.LedgerName,
                this.sessionClient,
                this.maxConcurrentTransactions,
                this.Logger,
                this.serializer);
        }
    }
}
