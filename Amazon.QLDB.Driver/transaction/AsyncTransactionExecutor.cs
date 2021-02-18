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
    /// Transaction object used within asynchronous lambda executions to provide a reduced view that allows only the operations that are
    /// valid within the context of an active managed transaction.
    /// </summary>
    public class AsyncTransactionExecutor
    {
        private readonly AsyncTransaction transaction;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncTransactionExecutor"/> class.
        /// </summary>
        ///
        /// <param name="transaction">The <see cref="AsyncTransaction"/> object the <see cref="AsyncTransactionExecutor"/> wraps.</param>
        internal AsyncTransactionExecutor(AsyncTransaction transaction)
        {
            this.transaction = transaction;
        }

        /// <summary>
        /// Execute the statement against QLDB and retrieve the result.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        ///
        /// <returns>Result from executed statement.</returns>
        ///
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        public Task<IAsyncResult> Execute(string statement)
        {
            return this.transaction.Execute(statement);
        }

        /// <summary>
        /// Execute the statement using the specified parameters against QLDB and retrieve the result.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="parameters">Parameters to execute.</param>
        ///
        /// <returns>Result from executed statement.</returns>
        ///
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        public Task<IAsyncResult> Execute(
            string statement, List<IIonValue> parameters)
        {
            return this.transaction.Execute(statement, parameters);
        }

        /// <summary>
        /// Execute the statement using the specified parameters against QLDB and retrieve the result.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="parameters">Parameters to execute.</param>
        ///
        /// <returns>Result from executed statement.</returns>
        ///
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        public Task<IAsyncResult> Execute(string statement, params IIonValue[] parameters)
        {
            return this.transaction.Execute(statement, parameters);
        }

        /// <summary>
        /// Abort the transaction and roll back any changes.
        /// </summary>
        public async Task Abort()
        {
            try
            {
                await this.transaction.Abort();
                throw new TransactionAbortedException(this.transaction.Id, true);
            }
            catch (AmazonServiceException ase)
            {
                throw new TransactionAbortedException(this.transaction.Id, false, ase);
            }
        }
    }
}
