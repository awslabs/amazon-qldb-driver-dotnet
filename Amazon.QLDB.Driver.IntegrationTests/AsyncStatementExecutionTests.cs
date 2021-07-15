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
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Amazon.IonDotnet.Builders;
    using Amazon.IonDotnet.Tree;
    using Amazon.IonDotnet.Tree.Impl;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using Amazon.QLDB.Driver.Serialization;

    [TestClass]
    public class AsyncStatementExecutionTests
    {
        private static readonly IValueFactory ValueFactory = new ValueFactory();
        private static AmazonQLDBSessionConfig amazonQldbSessionConfig;
        private static IntegrationTestBase integrationTestBase;
        private static AsyncQldbDriver qldbDriver;
        private static readonly IIonValue IonString = ValueFactory.NewString(Constants.SingleDocumentValue);
        private static readonly IIonValue IonString1 = ValueFactory.NewString(Constants.MultipleDocumentValue1);
        private static readonly IIonValue IonString2 = ValueFactory.NewString(Constants.MultipleDocumentValue2);

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
            qldbDriver = integrationTestBase.CreateAsyncDriver(amazonQldbSessionConfig, new ObjectSerializer());

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
        public async Task ExecuteAsync_DropExistingTable_TableDropped()
        {
            // Given.
            var createTableQuery = $"CREATE TABLE {Constants.CreateTableName}";
            Assert.AreEqual(1, await ExecuteAndReturnRowCount(createTableQuery));

            // Ensure table is created.
            Assert.IsTrue(await ConfirmTableExists(Constants.CreateTableName));

            // When.
            var dropTableQuery = $"DROP TABLE {Constants.CreateTableName}";
            Assert.AreEqual(1, await ExecuteAndReturnRowCount(dropTableQuery));

            // Then.
            Assert.IsFalse(await ConfirmTableExists(Constants.CreateTableName));
        }

        [TestMethod]
        public async Task ExecuteAsync_ListTables_ReturnsListOfTables()
        {
            // When.
            var result = await qldbDriver.ListTableNames();

            // Then.
            Assert.AreEqual(1, result.Count());

            Assert.IsTrue(result.Contains(Constants.TableName));
        }

        [TestMethod]
        [ExpectedException(typeof(BadRequestException))]
        public async Task ExecuteAsync_CreateTableThatAlreadyExist_ThrowBadRequestException()
        {
            // Given.
            var query = $"CREATE TABLE {Constants.TableName}";

            // When.
            await qldbDriver.Execute(async txn => await txn.Execute(query));
        }

        [TestMethod]
        public async Task ExecuteAsync_CreateIndex_IndexIsCreated()
        {
            // Given.
            var query = $"CREATE INDEX on {Constants.TableName} ({Constants.IndexAttribute})";

            // When.
            Assert.AreEqual(1, await ExecuteAndReturnRowCount(query));

            // Then.
            var searchQuery = $@"SELECT VALUE indexes[0] FROM information_schema.user_tables
                                  WHERE status = 'ACTIVE' AND name = '{Constants.TableName}'";
            var indexColumn = await ExecuteAndReturnField(searchQuery, "expr");

            Assert.AreEqual("[" + Constants.IndexAttribute + "]", indexColumn.StringValue);
        }

        [TestMethod]
        public async Task ExecuteAsync_QueryTableThatHasNoRecords_ReturnsEmptyResult()
        {
            // Given.
            var query = $"SELECT * FROM {Constants.TableName}";

            // When/Then.
            Assert.AreEqual(0, await ExecuteAndReturnRowCount(query));
        }

        [TestMethod]
        public async Task ExecuteAsync_InsertDocument_DocumentIsInserted()
        {
            // Given.
            // Create Ion struct to insert.
            IIonValue ionStruct = GetSingleValueIonStruct(IonString);

            // When.
            var insertQuery = $"INSERT INTO {Constants.TableName} ?";
            Assert.AreEqual(1, await ExecuteWithParamAndReturnRowCount(insertQuery, ionStruct));

            // Then.
            var searchQuery = $@"SELECT VALUE {Constants.ColumnName} FROM {Constants.TableName} 
                               WHERE {Constants.ColumnName} = '{Constants.SingleDocumentValue}'";

            var ionVal = await ExecuteAndReturnIonValue(searchQuery);
            Assert.AreEqual(Constants.SingleDocumentValue, ionVal.StringValue);
        }

        [TestMethod]
        public async Task Execute_InsertDocument_UsingObjectSerialization()
        {
            ParameterObject testObject = new ParameterObject();

            var query = $"INSERT INTO {Constants.TableName} ?";
            var count = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(txn.Query<ResultObject>(query, testObject));

                return await result.CountAsync();
            });
            Assert.AreEqual(1, count);

            var searchQuery = $"SELECT * FROM {Constants.TableName}";
            var searchResult = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(txn.Query<ParameterObject>(searchQuery));

                ParameterObject value = null;
                await foreach (var row in result)
                {
                    value = row;
                }
                return value;
            });

            Assert.AreEqual(testObject.ToString(), searchResult.ToString());
        }

        [TestMethod]
        public async Task ExecuteAsync_InsertDocumentWithMultipleFields_DocumentIsInserted()
        {
            // Given.
            // Create Ion struct to insert.
            IIonValue ionStruct = GetSingleValueIonStruct(IonString);
            ionStruct.SetField(Constants.SecondColumnName, ValueFactory.NewString(Constants.SingleDocumentValue));

            // When.
            var insertQuery = $"INSERT INTO {Constants.TableName} ?";
            Assert.AreEqual(1, await ExecuteWithParamAndReturnRowCount(insertQuery, ionStruct));

            // Then.
            var searchQuery = $@"SELECT {Constants.ColumnName}, {Constants.SecondColumnName} FROM {Constants.TableName} 
                               WHERE {Constants.ColumnName} = '{Constants.SingleDocumentValue}' 
                               AND  {Constants.SecondColumnName} = '{Constants.SingleDocumentValue}'";
            IIonValue value = await ExecuteAndReturnIonValue(searchQuery);

            var ionReader = IonReaderBuilder.Build(value);
            ionReader.MoveNext();
            ionReader.StepIn();
            ionReader.MoveNext();
            Assert.AreEqual(Constants.ColumnName, ionReader.CurrentFieldName);
            Assert.AreEqual(Constants.SingleDocumentValue, ionReader.StringValue());
            ionReader.MoveNext();
            Assert.AreEqual(Constants.SecondColumnName, ionReader.CurrentFieldName);
            Assert.AreEqual(Constants.SingleDocumentValue, ionReader.StringValue());
        }

        [TestMethod]
        public async Task ExecuteAsync_QuerySingleField_ReturnsSingleField()
        {
            // Given.
            // Create Ion struct to insert.
            IIonValue ionStruct = GetSingleValueIonStruct(IonString);

            var insertQuery = $"INSERT INTO {Constants.TableName} ?";
            Assert.AreEqual(1, await ExecuteWithParamAndReturnRowCount(insertQuery, ionStruct));

            // When.
            var searchQuery = $@"SELECT VALUE {Constants.ColumnName} FROM {Constants.TableName}
                                 WHERE {Constants.ColumnName} = '{Constants.SingleDocumentValue}'";
            IIonValue ionVal = await ExecuteAndReturnIonValue(searchQuery);

            // Then.
            Assert.AreEqual(Constants.SingleDocumentValue, ionVal.StringValue);
        }

        [TestMethod]
        public async Task ExecuteAsync_QueryTableEnclosedInQuotes_ReturnsResult()
        {
            // Given.
            // Create Ion struct to insert.
            IIonValue ionStruct = GetSingleValueIonStruct(IonString);

            var insertQuery = $"INSERT INTO {Constants.TableName} ?";
            Assert.AreEqual(1, await ExecuteWithParamAndReturnRowCount(insertQuery, ionStruct));

            // When.
            var searchQuery = $@"SELECT VALUE {Constants.ColumnName} FROM ""{Constants.TableName}""
                                 WHERE {Constants.ColumnName} = '{Constants.SingleDocumentValue}'";
            IIonValue ionVal = await ExecuteAndReturnIonValue(searchQuery);

            // Then.
            Assert.AreEqual(Constants.SingleDocumentValue, ionVal.StringValue);
        }

        [TestMethod]
        public async Task ExecuteAsync_InsertMultipleDocuments_DocumentsInserted()
        {
            // Given.
            // Create Ion structs to insert as parameters.
            List<IIonValue> parameters =
                new List<IIonValue> { GetSingleValueIonStruct(IonString1), GetSingleValueIonStruct(IonString2) };

            // When.
            var query = $"INSERT INTO {Constants.TableName} <<?,?>>";
            Assert.AreEqual(2, await ExecuteWithParamsAndReturnRowCount(query, parameters));

            // Then.
            var searchQuery = $@"SELECT VALUE {Constants.ColumnName} FROM {Constants.TableName}
                                 WHERE {Constants.ColumnName} IN (?,?)";
            var values = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(searchQuery, IonString1, IonString2);

                var values = new List<String>();
                await foreach (var row in result)
                {
                    values.Add(row.StringValue);
                }
                return values;
            });
            Assert.IsTrue(values.Contains(Constants.MultipleDocumentValue1));
            Assert.IsTrue(values.Contains(Constants.MultipleDocumentValue2));
        }

        [TestMethod]
        public async Task ExecuteAsync_DeleteSingleDocument_DocumentIsDeleted()
        {
            // Given.
            // Create Ion struct to insert.
            IIonValue ionStruct = GetSingleValueIonStruct(IonString);

            var insertQuery = $"INSERT INTO {Constants.TableName} ?";
            Assert.AreEqual(1, await ExecuteWithParamAndReturnRowCount(insertQuery, ionStruct));

            // When.
            var deleteQuery = $@"DELETE FROM { Constants.TableName}
                                 WHERE {Constants.ColumnName} = '{Constants.SingleDocumentValue}'";
            Assert.AreEqual(1, await ExecuteAndReturnRowCount((deleteQuery)));

            // Then.
            var searchQuery = $"SELECT COUNT(*) FROM {Constants.TableName}";
            var searchCount = await ExecuteAndReturnField(searchQuery, "_1");
            Assert.AreEqual(0, searchCount.IntValue);
        }

        [TestMethod]
        public async Task ExecuteAsync_DeleteAllDocuments_DocumentsAreDeleted()
        {
            // Given.
            // Create Ion structs to insert as parameters.
            List<IIonValue> parameters =
                new List<IIonValue> { GetSingleValueIonStruct(IonString1), GetSingleValueIonStruct(IonString2) };

            var query = $"INSERT INTO {Constants.TableName} <<?,?>>";
            Assert.AreEqual(2, await ExecuteWithParamsAndReturnRowCount(query, parameters));

            // When.
            var deleteQuery = $"DELETE FROM { Constants.TableName}";
            Assert.AreEqual(2, await ExecuteAndReturnRowCount(deleteQuery));

            // Then.
            var searchQuery = $"SELECT COUNT(*) FROM {Constants.TableName}";
            var searchCount = await ExecuteAndReturnField(searchQuery, "_1");
            Assert.AreEqual(0, searchCount.IntValue);
        }

        [TestMethod]
        [ExpectedException(typeof(OccConflictException))]
        public async Task ExecuteAsync_UpdateSameRecordAtSameTime_ThrowsOccException()
        {
            // Create a driver that does not retry OCC errors
            AsyncQldbDriver driver = integrationTestBase.CreateAsyncDriver(amazonQldbSessionConfig, default, default);

            // Insert document.
            // Create Ion struct with int value 0 to insert.
            IIonValue ionStruct = GetSingleValueIonStruct(ValueFactory.NewInt(0));

            var insertQuery = $"INSERT INTO {Constants.TableName} ?";
            var insertCount = await driver.Execute(
                async txn => await (await txn.Execute(insertQuery, ionStruct)).CountAsync());
            Assert.AreEqual(1, insertCount);

            string selectQuery = $"SELECT VALUE {Constants.ColumnName} FROM {Constants.TableName}";
            string updateQuery = $"UPDATE {Constants.TableName} SET {Constants.ColumnName} = ?";

            RetryPolicy retryPolicy = RetryPolicy.Builder().WithMaxRetries(0).Build();

            // For testing purposes only. Forcefully causes an OCC conflict to occur.
            // Do not invoke QldbDriver.Execute within the lambda function under normal circumstances.
            await driver.Execute(async txn =>
            {
                // Query table.
                var result = await txn.Execute(selectQuery);

                var currentValue = 0;
                await foreach (var row in result)
                {
                    currentValue = row.IntValue;
                }

                await driver.Execute(async txn =>
                {
                    // Update document.
                    var ionValue = ValueFactory.NewInt(currentValue + 5);
                    await txn.Execute(updateQuery, ionValue);
                }, retryPolicy);
            }, retryPolicy);
        }

        [TestMethod]
        [CreateIonValues]
        public async Task ExecuteAsync_InsertAndReadIonTypes_IonTypesAreInsertedAndRead(IIonValue ionValue)
        {
            // Given.
            // Create Ion struct to be inserted.
            IIonValue ionStruct = GetSingleValueIonStruct(ionValue);

            var insertQuery = $"INSERT INTO {Constants.TableName} ?";
            Assert.AreEqual(1, await ExecuteWithParamAndReturnRowCount(insertQuery, ionStruct));

            // When.
            IIonValue searchResult;
            if (ionValue.IsNull)
            {
                var searchQuery = $@"SELECT VALUE { Constants.ColumnName } FROM { Constants.TableName }
                                     WHERE { Constants.ColumnName } IS NULL";
                searchResult = await ExecuteAndReturnIonValue(searchQuery);
            }
            else
            {
                var searchQuery = $@"SELECT VALUE { Constants.ColumnName } FROM { Constants.TableName }
                                     WHERE { Constants.ColumnName } = ?";
                searchResult = await ExecuteWithParamAndReturnIonValue(searchQuery, ionValue);
            }

            // Then.
            if (searchResult.Type() != ionValue.Type())
            {
                Assert.Fail($"The queried value type, { searchResult.Type().ToString() }," +
                    $"does not match { ionValue.Type().ToString() }.");
            }
        }

        [TestMethod]
        [CreateIonValues]
        public async Task ExecuteAsync_UpdateIonTypes_IonTypesAreUpdated(IIonValue ionValue)
        {
            // Given.
            // Create Ion struct to be inserted.
            IIonValue ionStruct = GetSingleValueIonStruct(ValueFactory.NewNull());

            // Insert first record which will be subsequently updated.
            var insertQuery = $"INSERT INTO {Constants.TableName} ?";
            Assert.AreEqual(1, await ExecuteWithParamAndReturnRowCount(insertQuery, ionStruct));

            // When.
            var updateQuery = $"UPDATE { Constants.TableName } SET { Constants.ColumnName } = ?";
            Assert.AreEqual(1, await ExecuteWithParamAndReturnRowCount(updateQuery, ionValue));

            // Then.
            IIonValue searchResult;
            if (ionValue.IsNull)
            {
                var searchQuery = $@"SELECT VALUE { Constants.ColumnName } FROM { Constants.TableName }
                                     WHERE { Constants.ColumnName } IS NULL";
                searchResult = await ExecuteAndReturnIonValue(searchQuery);
            }
            else
            {
                var searchQuery = $@"SELECT VALUE { Constants.ColumnName } FROM { Constants.TableName }
                                     WHERE { Constants.ColumnName } = ?";
                searchResult = await ExecuteWithParamAndReturnIonValue(searchQuery, ionValue);
            }

            if (searchResult.Type() != ionValue.Type())
            {
                Assert.Fail($"The queried value type, { searchResult.Type().ToString() }," +
                    $"does not match { ionValue.Type().ToString() }.");
            }
        }

        [TestMethod]
        public async Task ExecuteAsync_ExecuteLambdaThatDoesNotReturnValue_RecordIsUpdated()
        {
            // Given.
            // Create Ion struct to insert.
            IIonValue ionStruct = GetSingleValueIonStruct(IonString);

            // When.
            var insertQuery = $"INSERT INTO {Constants.TableName} ?";
            await qldbDriver.Execute(async txn => await txn.Execute(insertQuery, ionStruct));

            // Then.
            var searchQuery = $@"SELECT VALUE {Constants.ColumnName} FROM {Constants.TableName}
                                 WHERE {Constants.ColumnName} = '{Constants.SingleDocumentValue}'";
            var ionVal = await ExecuteAndReturnIonValue(searchQuery);
            Assert.AreEqual(Constants.SingleDocumentValue, ionVal.StringValue);
        }

        [TestMethod]
        [ExpectedException(typeof(BadRequestException))]
        public async Task ExecuteAsync_DeleteTableThatDoesntExist_ThrowsBadRequestException()
        {
            // Given.
            var query = "DELETE FROM NonExistentTable";

            // When.
            await qldbDriver.Execute(async txn => await txn.Execute(query));
        }

        [TestMethod]
        public async Task ExecuteAsync_ExecutionMetrics()
        {
            var insertQuery = String.Format("INSERT INTO {0} << {{'col': 1}}, {{'col': 2}}, {{'col': 3}} >>",
               Constants.TableName);

            await qldbDriver.Execute(async txn => await txn.Execute(insertQuery));

            // Given
            var selectQuery = String.Format("SELECT * FROM {0} as a, {0} as b, {0} as c, {0} as d, {0} as e, {0} as f",
                Constants.TableName);

            // When
            await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(selectQuery);
                long readIOs = 0;
                long processingTime = 0;

                await foreach (IIonValue row in result)
                {
                    var ioUsage = result.GetConsumedIOs();
                    if (ioUsage != null)
                        readIOs = ioUsage.Value.ReadIOs;
                    var timingInfo = result.GetTimingInformation();
                    if (timingInfo != null)
                        processingTime = timingInfo.Value.ProcessingTimeMilliseconds;
                }

                // The 1092 value is from selectQuery, that performs self joins on a table.
                Assert.AreEqual(1092, readIOs);
                Assert.IsTrue(processingTime > 0);
            });

            // When
            var result = await qldbDriver.Execute(async txn => await txn.Execute(selectQuery));

            var ioUsage = result.GetConsumedIOs();
            var timingInfo = result.GetTimingInformation();

            Assert.IsNotNull(ioUsage);
            Assert.IsNotNull(timingInfo);

            // The 1092 value is from selectQuery, that performs self joins on a table.
            Assert.AreEqual(1092, ioUsage?.ReadIOs);
            Assert.IsTrue(timingInfo?.ProcessingTimeMilliseconds > 0);
        }
        
        [TestMethod]
        public async Task ExecuteAsync_ReturnTransactionIdAfterStatementExecution()
        {
            var query = $"SELECT * FROM {Constants.TableName}";
            var txnId = await qldbDriver.Execute(async txn =>
            {
                await txn.Execute(query);

                return txn.Id;
            });
            
            Assert.IsNotNull(txnId);
            Assert.IsTrue(txnId.Length > 0);
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

        private static async Task<int> ExecuteWithParamAndReturnRowCount(string statement, IIonValue param)
        {
            return await ExecuteWithParamsAndReturnRowCount(statement, new List<IIonValue>{ param });
        }

        private static async Task<int> ExecuteWithParamsAndReturnRowCount(string statement, List<IIonValue> parameters)
        {
            return await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(statement, parameters);

                return await result.CountAsync();
            });
        }

        private static async Task<IIonValue> ExecuteAndReturnIonValue(string statement)
        {
            return await ExecuteWithParamsAndReturnIonValue(statement, new List<IIonValue>());
        }

        private static async Task<IIonValue> ExecuteWithParamAndReturnIonValue(string statement, IIonValue param)
        {
            return await ExecuteWithParamsAndReturnIonValue(statement, new List<IIonValue>{ param });
        }

        private static async Task<IIonValue> ExecuteWithParamsAndReturnIonValue(string statement, List<IIonValue> parameters)
        {
            return await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(statement, parameters);

                int count = 0;
                IIonValue value = null;
                await foreach (var row in result)
                {
                    count++;
                    value = row;
                }

                // Confirm we have only 1 row in result set.
                Assert.AreEqual(1, count);

                return value;
            });
        }

        private static async Task<IIonValue> ExecuteAndReturnField(string statement, string fieldName)
        {
            return await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(statement);

                int count = 0;
                IIonValue ionValue = null;
                await foreach (var row in result)
                {
                    count++;
                    ionValue = row.GetField(fieldName);
                }

                // Confirm we have only 1 row in result set.
                Assert.AreEqual(1, count);

                return ionValue;
            });
        }

        private static IIonValue GetSingleValueIonStruct(IIonValue ionValue)
        {
            IIonValue ionStruct = ValueFactory.NewEmptyStruct();
            ionStruct.SetField(Constants.ColumnName, ionValue);

            return ionStruct;
        }
    }
}
