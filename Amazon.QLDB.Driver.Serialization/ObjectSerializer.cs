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
    using System;
    using System.IO;
    using Amazon.Ion.ObjectMapper;
    using Amazon.QLDBSession.Model;

    public class ObjectSerializer : ISerializer
    {
        private readonly IonSerializer serializer;

        public ObjectSerializer()
        {
            serializer = new IonSerializer();
        }

        public T Deserialize<T>(ValueHolder v)
        {
            return serializer.Deserialize<T>(v.IonBinary);
        }

        public ValueHolder Serialize(object o)
        {
            MemoryStream memoryStream = new MemoryStream();
            serializer.Serialize(o).CopyTo(memoryStream);
            memoryStream.Flush();
            memoryStream.Position = 0;
            return new ValueHolder
            {
                IonBinary = memoryStream,
            };
        }
    }
    
}
