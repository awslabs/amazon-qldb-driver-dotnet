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
    using System.Text.RegularExpressions;
    using Amazon.QLDBSession.Model;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Base class for Retry Handlers.
    /// </summary>
    internal abstract class BaseRetryHandler
    {
        private protected readonly ILogger logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="BaseRetryHandler"/> class.
        /// </summary>
        ///
        /// <param name="logger">Logger for this retry handler to use.</param>
        internal BaseRetryHandler(ILogger logger)
        {
            this.logger = logger;
        }

        internal static bool IsTransactionExpiry(Exception ex)
        {
            return ex is InvalidSessionException
                   && Regex.Match(ex.Message, @"Transaction\s.*\shas\sexpired").Success;
        }

        internal static string TryGetTransactionId(Exception ex)
        {
            return ex is QldbTransactionException exception ? exception.TransactionId : string.Empty;
        }
    }
}
