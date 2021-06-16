﻿/*
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

namespace Amazon.QLDB.Driver.IntegrationTests
{
    using Amazon.QLDBSession;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class StatementExecutionWithSerializationTests
    {
        private static AmazonQLDBSessionConfig amazonQldbSessionConfig;
        private static IntegrationTestBase integrationTestBase;
        private static QldbDriver qldbDriver;

        [ClassInitialize]
        public static void SetUp(TestContext context)
        {
            // Get AWS configuration properties from .runsettings file.
            string region = context.Properties["region"].ToString();
            const string ledgerName = "DotnetStatementExecution";

            amazonQldbSessionConfig = IntegrationTestBase.CreateAmazonQLDBSessionConfig(region);
            integrationTestBase = new IntegrationTestBase(ledgerName, region);

            integrationTestBase.RunForceDeleteLedger();

            integrationTestBase.RunCreateLedger();
            qldbDriver = integrationTestBase.CreateDriver(amazonQldbSessionConfig, new MySerialization());

            // Create table.
            var query = $"CREATE TABLE {Constants.TableName}";
            var count = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(query);

                var count = 0;
                foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, count);

            var result = qldbDriver.ListTableNames();
            foreach (var row in result)
            {
                Assert.AreEqual(Constants.TableName, row);
            }
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            qldbDriver.Dispose();
            integrationTestBase.RunForceDeleteLedger();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            // Delete all documents in table.
            qldbDriver.Execute(txn => txn.Execute($"DELETE FROM {Constants.TableName}"));
        }

        [TestMethod]
        public void Execute_InsertDocument_UsingObjectSerialization()
        {
            // Given.
            // Create a C# object to insert.
            ParameterObject testObject = new ParameterObject();

            // When.
            var query = $"INSERT INTO {Constants.TableName} ?";
            var count = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(txn.Query<ResultObject>(query, testObject));

                var count = 0;
                foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, count);
        }
    }
}
