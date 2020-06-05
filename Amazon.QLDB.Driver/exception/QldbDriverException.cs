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
    using System.Net;
    using Amazon.QLDBSession;
    using Amazon.Runtime;

    /// <summary>
    /// Exception type representing exceptions that originate from the QLDB driver, rather than QLDB itself.
    /// </summary>
    public class QldbDriverException : AmazonQLDBSessionException
    {
        internal QldbDriverException(string message)
            : base(message)
        {
        }

        internal QldbDriverException(Exception innerException)
            : base(innerException)
        {
        }

        internal QldbDriverException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        internal QldbDriverException(string message, ErrorType errorType, string errorCode, string requestId, HttpStatusCode statusCode)
            : base(message, errorType, errorCode, requestId, statusCode)
        {
        }

        internal QldbDriverException(string message, Exception innerException, ErrorType errorType, string errorCode, string requestId, HttpStatusCode statusCode)
            : base(message, innerException, errorType, errorCode, requestId, statusCode)
        {
        }
    }
}
