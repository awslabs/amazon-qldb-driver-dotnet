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
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Amazon.IonDotnet.Tree;
    using Amazon.Runtime;

    /// <summary>
    /// Interface for asynchronous executions of a statement within an active transaction to QLDB.
    /// </summary>
    public interface IAsyncExecutable
    {
        /// <summary>
        /// Execute the statement asynchronously against QLDB and retrieve the result.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        ///
        /// <returns>Result from executed statement.</returns>
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        Task<IAsyncResult> Execute(string statement);

        /// <summary>
        /// Execute the statement asynchronously using the specified parameters against QLDB and retrieve the result.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="parameters">Parameters to execute.</param>
        ///
        /// <returns>Result from executed statement.</returns>
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        Task<IAsyncResult> Execute(string statement, List<IIonValue> parameters);

        /// <summary>
        /// Execute the statement asynchronously using the specified parameters against QLDB and retrieve the result.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="parameters">Parameters to execute.</param>
        ///
        /// <returns>Result from executed statement.</returns>
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        Task<IAsyncResult> Execute(string statement, params IIonValue[] parameters);
    }
}
