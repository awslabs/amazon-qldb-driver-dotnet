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

namespace Amazon.QLDB.Driver.IntegrationTests
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Amazon.QLDBSession;
    using Amazon.IonDotnet.Tree;
    using Amazon.IonDotnet.Tree.Impl;
    using Microsoft.VisualStudio.TestTools.UnitTesting;

    [TestClass]
    public class AsyncStatementExecutionWithSerializationTests
    {
        private static readonly IValueFactory ValueFactory = new ValueFactory();
        private static AmazonQLDBSessionConfig amazonQldbSessionConfig;
        private static IntegrationTestBase integrationTestBase;
        private static AsyncQldbDriver qldbDriver;

        [ClassInitialize]
        public static async Task SetUp(TestContext context)
        {
            // Get AWS configuration properties from .runsettings file.
            string region = context.Properties["region"].ToString();
            const string ledgerName = "DotnetAsyncStatementExecution";

            amazonQldbSessionConfig = IntegrationTestBase.CreateAmazonQLDBSessionConfig(region);
            integrationTestBase = new IntegrationTestBase(ledgerName, region);

            integrationTestBase.RunForceDeleteLedger();

            integrationTestBase.RunCreateLedger();
            qldbDriver = integrationTestBase.CreateAsyncDriver(amazonQldbSessionConfig, new MySerialization());

            // Create table.
            var query = $"CREATE TABLE {Constants.TableName}";

            Assert.AreEqual(1, await ExecuteAndReturnRowCount(query));

            Assert.IsTrue(await ConfirmTableExists(Constants.TableName));
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            qldbDriver.Dispose();
            integrationTestBase.RunForceDeleteLedger();
        }

        [TestCleanup]
        public async Task TestCleanup()
        {
            // Delete all documents in table.
            await qldbDriver.Execute(async txn => await txn.Execute($"DELETE FROM {Constants.TableName}"));
        }

        [TestMethod]
        public async Task ExecuteAsync_InsertDocument_UsingObjectSerialization()
        {
            // Given.
            // Create a C# object to insert.
            ParameterObject testObject = new ParameterObject();

            // When.
            var query = $"INSERT INTO {Constants.TableName} ?";
            var count = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(txn.Query<ResultObject>(query, testObject));

                return await result.CountAsync();
            });
            Assert.AreEqual(1, count);
        }

        private static async Task<bool> ConfirmTableExists(string tableName)
        {
            var result = await qldbDriver.ListTableNames();

            var tables = result.ToList();

            return tables.Contains(tableName);
        }

        private static async Task<int> ExecuteAndReturnRowCount(string statement)
        {
            return await ExecuteWithParamsAndReturnRowCount(statement, new List<IIonValue>());
        }

        private static async Task<int> ExecuteWithParamsAndReturnRowCount(string statement, List<IIonValue> parameters)
        {
            return await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(statement, parameters);

                return await result.CountAsync();
            });
        }
    }
}
