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
    using Amazon.QLDBSession.Model;

    /// <summary>
    /// Interface for the Ion serializer.
    /// </summary>
    public interface ISerializer
    {
        /// <summary>
        /// Serialize a C# object into a ValueHolder object containing the Ion binary or text value.
        /// </summary>
        ///
        /// <param name="o">The C# object to be serialized into ValueHolder.</param>
        ///
        /// <returns>The ValueHolder object containing the Ion binary or text value.</returns>
        ValueHolder Serialize(object o);

        /// <summary>
        /// Deserialize a ValueHolder object containing the Ion binary or text value into an object of type T.
        /// </summary>
        ///
        /// <param name="s">The ValueHolder object to be deserialized into an object of type T.</param>
        /// <typeparam name="T">The return type.</typeparam>
        ///
        /// <returns>The object of type T.</returns>
        T Deserialize<T>(ValueHolder s);
    }
}
