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
    using System.Text.RegularExpressions;
    using Amazon.QLDBSession.Model;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Base class for QLDB Session.
    /// </summary>
    internal abstract class BaseQldbSession
    {
        private protected readonly ILogger logger;
        private protected Session session;
        private protected readonly ISerializer serializer;

        internal BaseQldbSession(Session session, ILogger logger, ISerializer serializer)
        {
            this.session = session;
            this.logger = logger;
            this.serializer = serializer;
        }

        /// <summary>
        /// End the underlying internal session.
        /// </summary>
        internal void End()
        {
            this.session.End();
        }

        /// <summary>
        /// Retrieve the ID of this session.
        /// </summary>
        ///
        /// <returns>The ID of this session.</returns>
        internal string GetSessionId()
        {
            return this.session.SessionId;
        }

        private protected static bool IsTransactionExpiredException(InvalidSessionException ise)
        {
            return Regex.Match(ise.Message, @"Transaction\s.*\shas\sexpired").Success;
        }
    }
}
