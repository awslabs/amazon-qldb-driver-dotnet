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
    /// RetryPolicy used to retry the transactions.
    /// The default max retries is 4, and the default backoff strategy is <see cref="ExponentBackoffStrategy"/>.
    /// </summary>
    public class RetryPolicy
    {
        /// <summary>
        /// Gets the backoff strategy.
        /// </summary>
        public IBackoffStrategy BackoffStrategy { get; private set; }

        /// <summary>
        /// Gets the maximum number of retries.
        /// </summary>
        public int MaxRetries { get; private set; }

        /// <summary>
        /// Gets the Builder of the <see cref="RetryPolicy"/> class.
        /// </summary>
        /// <returns>The builder of <see cref="RetryPolicy"/>.</returns>
        public static RetryPolicyBuilder Builder()
        {
            return new RetryPolicyBuilder();
        }

        /// <summary>
        /// The builder class of <see cref="RetryPolicy"/>.
        /// </summary>.
        public class RetryPolicyBuilder
        {
            private const int DefaultMaxRetries = 4;
            private static readonly IBackoffStrategy DefaultBackoffStrategy = new ExponentBackoffStrategy();

            private int maxRetries;
            private IBackoffStrategy backoffStrategy;

            internal RetryPolicyBuilder()
            {
                this.maxRetries = DefaultMaxRetries;
                this.backoffStrategy = DefaultBackoffStrategy;
            }

            /// <summary>
            /// Build a <see cref="RetryPolicy"/> instance.
            /// </summary>
            /// <returns>A <see cref="RetryPolicy"/> instance.</returns>
            public RetryPolicy Build()
            {
                return new RetryPolicy()
                {
                    BackoffStrategy = this.backoffStrategy,
                    MaxRetries = this.maxRetries,
                };
            }

            /// <summary>
            /// Sets the maximum number of retries. If not called, the default value is 4.
            /// </summary>
            /// <param name="maxRetries">The maximum number of retries.</param>
            /// <returns>The builder instance.</returns>
            public RetryPolicyBuilder WithMaxRetries(int maxRetries)
            {
                this.maxRetries = maxRetries;
                return this;
            }

            /// <summary>
            /// Sets the backoff strategy of the retry policy. If not called, the default
            /// <see cref="ExponentBackoffStrategy"/> would be used.
            /// </summary>
            /// <param name="backoffStrategy">The backoff strategy.</param>
            /// <returns>The builder instance.</returns>
            public RetryPolicyBuilder WithBackoffStrategy(IBackoffStrategy backoffStrategy)
            {
                this.backoffStrategy = backoffStrategy;
                return this;
            }
        }
    }
}
