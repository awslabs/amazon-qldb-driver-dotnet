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
    using System;
    using System.IO;
    using System.Linq;
    using Amazon.QLDBSession.Model;

    /// <summary>
    /// An object encapsulating a query which can be executed against QLDB. The generic type T is the type 
    /// of each document returned from the database.
    /// </summary>
    public interface IQuery<T>
    {
        /// <summary>
        /// The statement string to execute.
        /// </summary>
        string Statement { get; }

        /// <summary>
        /// The Ion binary or text value for each parameter.
        /// </summary>
        ValueHolder[] Parameters { get; }

        /// <summary>
        /// Given an Ion ValueHolder, deserialize it into an object of type T.
        /// </summary>
        T Deserialize(ValueHolder ionValueHolder);
    }

    internal class QueryData<T> : IQuery<T>
    {
        private readonly object[] parameters;
        private readonly ISerializer serializer;

        internal QueryData(string statement, object[] parameters, ISerializer serializer)
        {
            Statement = statement;
            this.parameters = parameters;
            this.serializer = serializer;
        }

        public string Statement { get; }

        public ValueHolder[] Parameters
        {
            get {
                return parameters.Select(serializer.Serialize).ToArray();
            }
        }

        public T Deserialize(ValueHolder ionValueHolder)
        {
            return serializer.Deserialize<T>(ionValueHolder);
        }
    }
}
