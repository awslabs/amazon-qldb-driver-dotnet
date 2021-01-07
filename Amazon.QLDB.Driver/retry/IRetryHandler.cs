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
    using System;

    /// <summary>
    /// Interface of Retry Handler.
    /// </summary>
    internal interface IRetryHandler
    {
        /// <summary>
        /// Execute a retriable function.
        /// </summary>
        /// <typeparam name="T">The return type of the executed function.</typeparam>
        /// <param name="func">The function to be executed and retried if needed.</param>
        /// <param name="retryPolicy">The retry policy.</param>
        /// <param name="newSessionAction">The action to move to a new session.</param>
        /// <param name="nextSessionAction">The action to get the next session.</param>
        /// <param name="retryAction">The custom retry action.</param>
        /// <returns>The return value of the executed function.</returns>
        T RetriableExecute<T>(Func<T> func, RetryPolicy retryPolicy, Action newSessionAction, Action nextSessionAction, Action<int> retryAction);
    }
}
