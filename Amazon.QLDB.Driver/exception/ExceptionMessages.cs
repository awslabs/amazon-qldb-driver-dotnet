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
    internal static class ExceptionMessages
    {
        internal const string DriverClosed = "Operation is invalid as this QldbDriver has already been disposed.";
        internal const string ResultEnumeratorRetrieved = "The Enumerator of a Result can only be retrieved once. Please execute a new statement or buffer the results.";
        internal const string SessionClosed = "Operation is invalid as this QldbSession has already been disposed.";
        internal const string TransactionClosed = "Operation is invalid as this Transaction has been closed after a commit or abort operation, and cannot be reused.";
        internal const string TransactionDigestMismatch = "Transaction's commit digest did not match returned value from QLDB. Please retry with a new transaction.";
    }
}
