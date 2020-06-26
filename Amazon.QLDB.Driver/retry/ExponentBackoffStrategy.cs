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

    /// <summary>
    /// The exponential backoff strategy with an equal jitter.
    /// </summary>
    public class ExponentBackoffStrategy : IBackoffStrategy
    {
        public const int DefaultSleepBaseMilliseconds = 10;
        public const int DefaultSleepCapMilliseconds = 5000;

        private readonly int sleepBaseMilliseconds;
        private readonly int sleepCapMilliseconds;

        /// <summary>
        /// Initializes a new instance of the <see cref="ExponentBackoffStrategy"/> class.
        /// </summary>
        /// <param name="sleepBaseMilliseconds">The base of the exponent in milliseconds.</param>
        /// <param name="sleepCapMilliseconds">The cap of the delay in milliseconds.</param>
        public ExponentBackoffStrategy(int sleepBaseMilliseconds, int sleepCapMilliseconds)
        {
            ValidationUtils.AssertPositive(sleepBaseMilliseconds, "sleepBaseMilliseconds");
            ValidationUtils.AssertPositive(sleepCapMilliseconds, "sleepCapMilliseconds");
            ValidationUtils.AssertNotGreater(sleepBaseMilliseconds, sleepCapMilliseconds, "sleepBaseMilliseconds", "sleepCapMilliseconds");

            this.sleepBaseMilliseconds = sleepBaseMilliseconds;
            this.sleepCapMilliseconds = sleepCapMilliseconds;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ExponentBackoffStrategy"/> class with
        /// default settings.
        /// </summary>
        public ExponentBackoffStrategy()
            : this(DefaultSleepBaseMilliseconds, DefaultSleepCapMilliseconds)
        {
        }

        /// <summary>
        /// Calculate the delay based on the number of retries attempted. It has an equal jitter,
        /// which means the delay would be in the 90% to 100% range of the power.
        /// </summary>
        /// <param name="retryPolicyContext">The context of retry policy.</param>
        /// <returns>The calculated delay.</returns>
        public TimeSpan CalculateDelay(RetryPolicyContext retryPolicyContext)
        {
            var jitterRand = (new Random().NextDouble() * 0.5) + 0.5;
            var exponentialBackoff = Math.Min(this.sleepCapMilliseconds, Math.Pow(this.sleepBaseMilliseconds, retryPolicyContext.RetriesAttempted));

            return TimeSpan.FromMilliseconds(jitterRand * exponentialBackoff);
        }
    }
}
