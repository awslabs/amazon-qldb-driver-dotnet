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

namespace Amazon.QLDB.Driver.Tests
{
    using System.Diagnostics;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class EndToEndTest
    {
        // TODO: Remove
        // Currently for dev purposes only
        [TestMethod]
        public void EndToEnd()
        {
            var builder = QldbDriver.Builder();
            var driver = builder
                .WithLedger("vehicle-registration")
                .Build();
            var session = driver.GetSession();
            var tableNames = session.ListTableNames();
            foreach (var table in tableNames)
            {
                Debug.WriteLine(table);
            }
        }
    }
}
