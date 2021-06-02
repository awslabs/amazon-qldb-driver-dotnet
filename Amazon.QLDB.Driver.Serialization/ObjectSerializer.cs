using System;
using System.IO;
using Amazon.Ion.ObjectMapper;
using Amazon.QLDBSession.Model;

namespace Amazon.QLDB.Driver.Serialization
{
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
