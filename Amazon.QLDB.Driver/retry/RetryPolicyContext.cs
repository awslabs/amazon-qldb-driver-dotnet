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
    /// Data struct used to pass retry policy context.
    /// </summary>
    public struct RetryPolicyContext
    {
        public RetryPolicyContext(int retriesAttempted, Exception lastException)
        {
            this.RetriesAttempted = retriesAttempted;
            this.LastException = lastException;
        }

        public int RetriesAttempted { get; }

        public Exception LastException { get; }
    }
}
