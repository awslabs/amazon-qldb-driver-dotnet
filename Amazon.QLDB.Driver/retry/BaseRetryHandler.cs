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
    /// <para>The base class for Retry Handler.</para>
    ///
    /// <para>The driver retries in two scenarios: retrying inside a session, and retrying with another session. In the second case,
    /// it would require a <i>recover</i> action to reset the session into a working state.
    /// </summary>
    abstract class BaseRetryHandler
    {
        private readonly ILogger logger;

        internal static bool IsTransactionExpiry(Exception ex)
        {
            return ex is InvalidSessionException
                && Regex.Match(ex.Message, @"Transaction\s.*\shas\sexpired").Success;
        }

        protected static string TryGetTransactionId(Exception ex)
        {
            return ex is QldbTransactionException exception ? exception.TransactionId : string.Empty;
        }
    }
}
