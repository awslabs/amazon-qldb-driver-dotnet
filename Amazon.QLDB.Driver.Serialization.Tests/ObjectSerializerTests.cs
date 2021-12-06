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

namespace Amazon.QLDB.Driver.Serialization.Tests
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    class Person
    {
        public string Name { get; set; }
        public int Age { get; set; }

        public override string ToString()
        {
            return "<Person>{ Name: " + Name + ", Age: " + Age + " }";
        }
    }

    [TestClass]
    public class ObjectSerializerTests
    {
        private static readonly Person John = new Person { Name = "John", Age = 13 };

        [TestMethod]
        public void TestSerde()
        {
            ObjectSerializer serializer = new ObjectSerializer();
            Person testPerson = serializer.Deserialize<Person>(serializer.Serialize(John));

            Assert.AreEqual(John.ToString(), testPerson.ToString());
        }
    }
}


