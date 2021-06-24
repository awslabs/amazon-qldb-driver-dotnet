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

namespace Amazon.QLDB.Driver.Serialization
{
    using System.IO;
    using Amazon.IonObjectMapper;
    using Amazon.QLDBSession.Model;

    /// <summary>
    /// Serializer that serialize/deserialize C# objects to ValueHolder object containing Ion data.
    /// This uses the <see cref="IonObjectMapper"></see> library to perform the serialization/deserialization.
    /// </summary>
    public class ObjectSerializer : ISerializer
    {
        private readonly IonSerializer serializer;

        /// <summary>
        /// Initializes a new instance of the <see cref="ObjectSerializer"/> class.
        /// </summary>
        public ObjectSerializer()
        {
            serializer = new IonSerializer(new IonSerializationOptions{ NamingConvention = new TitleCaseNamingConvention() });
        }

        /// <summary>
        /// Deserialize a ValueHolder object containing the Ion binary into an object of type T.
        /// </summary>
        ///
        /// <param name="v">The ValueHolder object to be deserialized into an object of type T.</param>
        /// <typeparam name="T">The return type.</typeparam>
        ///
        /// <returns>The object of type T.</returns>
        public T Deserialize<T>(ValueHolder v)
        {
            return serializer.Deserialize<T>(v.IonBinary);
        }

        /// <summary>
        /// Serialize a C# object into a ValueHolder object containing the Ion binary.
        /// </summary>
        ///
        /// <param name="o">The C# object to be serialized into ValueHolder.</param>
        ///
        /// <returns>The ValueHolder object containing the Ion binary.</returns>
        public ValueHolder Serialize(object o)
        {
            MemoryStream memoryStream = new MemoryStream();
            serializer.Serialize(memoryStream, o);
            memoryStream.Flush();
            memoryStream.Position = 0;
            return new ValueHolder
            {
                IonBinary = memoryStream,
            };
        }
    }
}
