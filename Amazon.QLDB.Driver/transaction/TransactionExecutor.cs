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
    using Amazon.IonDotnet.Tree;
    using Amazon.Runtime;

    /// <summary>
    /// Transaction object used within lambda executions to provide a reduced view that allows only the operations that
    /// are valid within the context of an active managed transaction.
    /// </summary>
    public class TransactionExecutor : IExecutable
    {
        private readonly Transaction transaction;
        private readonly ISerializer serializer;

        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionExecutor"/> class.
        /// </summary>
        ///
        /// <param name="transaction">
        /// The <see cref="Transaction"/> object the <see cref="TransactionExecutor"/> wraps.
        /// </param>
        /// <param name="serializer">The serializer to serialize and deserialize Ion data.</param>
        internal TransactionExecutor(Transaction transaction, ISerializer serializer = null)
        {
            this.transaction = transaction;
            this.serializer = serializer;
        }

        /// <summary>
        /// Gets the transaction ID.
        /// </summary>
        public string Id => this.transaction.Id;

        /// <summary>
        /// Abort the transaction and roll back any changes.
        /// </summary>
        public void Abort()
        {
            try
            {
                this.transaction.Abort();
                throw new TransactionAbortedException(this.transaction.Id, true);
            }
            catch (AmazonServiceException ase)
            {
                throw new TransactionAbortedException(this.transaction.Id, false, ase);
            }
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
        public IResult Execute(string statement)
        {
            return this.transaction.Execute(statement);
        }

        /// <summary>
        /// Execute the IQuery against QLDB and retrieve the result.
        /// </summary>
        ///
        /// <param name="query">The query object containing the PartiQL statement and parameters to be executed against QLDB.</param>
        /// <typeparam name="T">The return type.</typeparam>
        ///
        /// <returns>Result from executed statement.</returns>
        ///
        /// <exception cref="AmazonServiceException">Thrown when there is an error executing against QLDB.</exception>
        public Generic.IResult<T> Execute<T>(IQuery<T> query)
        {
            return this.transaction.Execute(query);
        }

        /// <summary>
        /// Create a Query object containing the PartiQL statement and parameters to be executed against QLDB.
        /// </summary>
        ///
        /// <param name="statement">The PartiQL statement to be executed against QLDB.</param>
        /// <param name="parameters">Parameters to execute.</param>
        /// <typeparam name="T">The return type.</typeparam>
        ///
        /// <returns>A Query object.</returns>
        public IQuery<T> Query<T>(string statement, params object[] parameters)
        {
            return new QueryData<T>(statement, parameters, this.serializer);
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
        public IResult Execute(string statement, List<IIonValue> parameters)
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
        public IResult Execute(string statement, params IIonValue[] parameters)
        {
            return this.transaction.Execute(statement, parameters);
        }
    }
}
