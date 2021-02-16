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
    /// Base class for QLDB Session.
    /// </summary>
    internal abstract class BaseQldbSession
    {
        private protected readonly ILogger logger;
        private protected Session session;
        private protected bool isAlive;

        internal BaseQldbSession(Session session, ILogger logger)
        {
            this.session = session;
            this.logger = logger;
            this.isAlive = true;
        }

        internal bool IsAlive()
        {
            return this.isAlive;
        }

        /// <summary>
        /// Close the internal session object.
        /// </summary>
        internal void Close()
        {
            this.session.End();
        }

        /// <summary>
        /// Abstract method for releasing the session which can still be used by another transaction.
        /// </summary>
        internal abstract void Release();

        /// <summary>
        /// Retrieve the ID of this session..
        /// </summary>
        ///
        /// <returns>The ID of this session.</returns>
        internal string GetSessionId()
        {
            return this.session.SessionId;
        }
    }
}
