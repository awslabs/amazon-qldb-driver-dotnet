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
    using System.Threading.Tasks;

    internal interface IAsyncTransaction : IAsyncExecutable
    {
        string Id { get; }

        /// <summary>
        /// Abort the transaction asynchronously and roll back any changes. No-op if closed.
        /// Any open <see cref="IAsyncResult"/> created by the transaction will be invalidated.
        /// </summary>
        ///
        /// <returns>A task representing the asynchronous abort operation.</returns>
        Task Abort();

        /// <summary>
        /// Commit the transaction asynchronously. Any open <see cref="IAsyncResult"/> created by the transaction will be invalidated.
        /// </summary>
        ///
        /// <exception cref="InvalidOperationException">Thrown when Hash returned from QLDB is not equal.</exception>
        /// <exception cref="OccConflictException">Thrown if an OCC conflict has been detected within the transaction.</exception>
        /// <exception cref="AmazonServiceException">Thrown when there is an error committing this transaction against QLDB.</exception>
        /// <exception cref="QldbDriverException">Thrown when this transaction has been disposed.</exception>
        /// <returns>A task representing the asynchronous commit operation.</returns>
        Task Commit();

    }
}
