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
    using System.Collections.Generic;
    using IonDotnet.Tree;

    /// <summary>
    /// Transaction object used within lambda executions to provide a reduced view that allows only the operations that are
    /// valid within the context of an active managed transaction.
    /// </summary>
    public class TransactionExecutor
    {
        private readonly ITransaction transaction;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="transaction">The transaction object the TransactionExecutor wraps.</param>
        internal TransactionExecutor(ITransaction transaction)
        {
            this.transaction = transaction;
        }

        /// <summary>
        /// Abort the transaction and roll back any changes.
        /// </summary>
        public void Abort()
        {
            throw new AbortException();
        }

        /// <summary>
        /// Execute the statement using the specified parameters against QLDB and retrieve the result.
        /// </summary>
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="parameters">The parameters to be used with the PartiQL statement.</param>
        ///
        /// <returns>The result of executing the statement.</returns>
        public IResult Execute(string statement, List<IIonValue> parameters = null)
        {
            return this.transaction.Execute(statement, parameters);
        }
    }
}
