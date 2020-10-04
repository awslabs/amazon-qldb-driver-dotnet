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
    using Amazon.QLDB.Driver.IntegrationTests.utils;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Amazon.IonDotnet.Builders;
    using Amazon.IonDotnet.Tree;
    using Amazon.IonDotnet.Tree.Impl;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    [TestClass]
    public class StatementExecutionTests
    {
        private static readonly IValueFactory ValueFactory = new ValueFactory();
        private static AmazonQLDBSessionConfig amazonQldbSessionConfig;
        private static IntegrationTestBase integrationTestBase;
        private static QldbDriver qldbDriver;

        [ClassInitialize]
        public static async Task SetUp(TestContext context)
        {
            // Get AWS configuration properties from .runsettings file.
            string region = context.Properties["region"].ToString();

            amazonQldbSessionConfig = IntegrationTestBase.CreateAmazonQLDBSessionConfig(region);
            integrationTestBase = new IntegrationTestBase(Constants.LedgerName, region);

            integrationTestBase.RunForceDeleteLedger();

            integrationTestBase.RunCreateLedger();
            qldbDriver = integrationTestBase.CreateDriver(amazonQldbSessionConfig);

            // Create table.
            var query = $"CREATE TABLE {Constants.TableName}";
            var count = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(query);

                var count = 0;
                await foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, count);

            var result = await qldbDriver.ListTableNames();
            foreach (var row in result)
            {
                Assert.AreEqual(Constants.TableName, row);
            }
        }

        [ClassCleanup]
        public static async Task ClassCleanup()
        {
            integrationTestBase.RunDeleteLedger();
            await qldbDriver.DisposeAsync();
        }

        [TestCleanup]
        public async Task TestCleanup()
        {
            // Delete all documents in table.
            await qldbDriver.Execute(async txn => await txn.Execute($"DELETE FROM {Constants.TableName}"));
        }

        [TestMethod]
        public async Task Execute_DropExistingTable_TableDropped()
        {
            // Given.
            var create_table_query = $"CREATE TABLE {Constants.CreateTableName}";
            var create_table_count = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(create_table_query);

                var count = 0;
                await foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, create_table_count);

            // Execute ListTableNames() to ensure table is created.
            var result = await qldbDriver.ListTableNames();

            var tables = new List<string>();
            foreach (var row in result)
            {
                tables.Add(row);
            }
            Assert.IsTrue(tables.Contains(Constants.CreateTableName));

            // When.
            var drop_table_query = $"DROP TABLE {Constants.CreateTableName}";
            var drop_table_count = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(drop_table_query);

                var count = 0;
                await foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, drop_table_count);

            // Then.
            tables.Clear();
            var updated_tables_result = await qldbDriver.ListTableNames();
            foreach (var row in updated_tables_result)
            {
                tables.Add(row);
            }
            Assert.IsFalse(tables.Contains(Constants.CreateTableName));
        }

        [TestMethod]
        public async Task Execute_ListTables_ReturnsListOfTables()
        {
            // When.
            var result = await qldbDriver.ListTableNames();

            // Then.
            int count = 0;
            foreach (var row in result)
            {
                count++;
                Assert.AreEqual(Constants.TableName, row);
            }

            Assert.AreEqual(1, count);
        }

        [TestMethod]
        public async Task Execute_ListTables_IsCancelled()
        {
            // Given
            var cts = new CancellationTokenSource();
            var cancellationToken = cts.Token;
            cts.Cancel();

            // When.
            Func<Task> act = () => qldbDriver.ListTableNames(cancellationToken);

            // Then.
            await Assert.ThrowsExceptionAsync<TaskCanceledException>(act);
        }

        [TestMethod]
        [ExpectedException(typeof(BadRequestException))]
        public async Task Execute_CreateTableThatAlreadyExist_ThrowBadRequestException()
        {
            // Given.
            var query = $"CREATE TABLE {Constants.TableName}";

            // When.
            await qldbDriver.Execute(async txn => await txn.Execute(query));
        }

        [TestMethod]
        public async Task Execute_CreateIndex_IndexIsCreated()
        {
            // Given.
            var query = $"CREATE INDEX on {Constants.TableName} ({Constants.IndexAttribute})";

            // When.
            var count = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(query);

                var count = 0;
                await foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, count);

            // Then.
            var search_query = $@"SELECT VALUE indexes[0] FROM information_schema.user_tables
                                  WHERE status = 'ACTIVE' AND name = '{Constants.TableName}'";
            var indexColumn = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(search_query);

                // Extract the index name by querying the information_schema.
                /* This gives: 
                {
                    expr: "[MyColumn]"
                }
                */
                var indexColumn = "";
                await foreach (var row in result)
                {
                    indexColumn = row.GetField("expr").StringValue;
                }
                return indexColumn;
            });

            Assert.AreEqual("[" + Constants.IndexAttribute + "]", indexColumn);
        }

        [TestMethod]
        public async Task Execute_QueryTableThatHasNoRecords_ReturnsEmptyResult()
        {
            // Given.
            var query = $"SELECT * FROM {Constants.TableName}";

            // When.
            int resultSetSize = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(query);

                int count = 0;
                await foreach (var row in result)
                {
                    count++;
                }
                return count;
            });

            // Then.
            Assert.AreEqual(0, resultSetSize);
        }

        [TestMethod]
        public async Task Execute_InsertDocument_DocumentIsInserted()
        {
            // Given.
            // Create Ion struct to insert.
            IIonValue ionStruct = ValueFactory.NewEmptyStruct();
            ionStruct.SetField(Constants.ColumnName, ValueFactory.NewString(Constants.SingleDocumentValue));

            // When.
            var query = $"INSERT INTO {Constants.TableName} ?";
            var count = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(query, ionStruct);

                var count = 0;
                await foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, count);

            // Then.
            var searchQuery = $@"SELECT VALUE {Constants.ColumnName} FROM {Constants.TableName} 
                               WHERE {Constants.ColumnName} = '{Constants.SingleDocumentValue}'";
            var value = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(searchQuery);

                var value = "";
                await foreach (var row in result)
                {
                    value = row.StringValue;
                }
                return value;
            });
            Assert.AreEqual(Constants.SingleDocumentValue, value);
        }

        [TestMethod]
        public async Task Execute_InsertDocumentWithMultipleFields_DocumentIsInserted()
        {
            // Given.
            // Create Ion struct to insert.
            IIonValue ionStruct = ValueFactory.NewEmptyStruct();
            ionStruct.SetField(Constants.ColumnName, ValueFactory.NewString(Constants.SingleDocumentValue));
            ionStruct.SetField(Constants.SecondColumnName, ValueFactory.NewString(Constants.SingleDocumentValue));

            // When.
            var query = $"INSERT  INTO {Constants.TableName} ?";
            var count = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(query, ionStruct);

                var count = 0;
                await foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, count);

            // Then.
            var searchQuery = $@"SELECT {Constants.ColumnName}, {Constants.SecondColumnName} FROM {Constants.TableName} 
                               WHERE {Constants.ColumnName} = '{Constants.SingleDocumentValue}' AND  {Constants.SecondColumnName} = '{Constants.SingleDocumentValue}'";
            IIonValue value = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(searchQuery);

                IIonValue value = null;
                await foreach (var row in result)
                {
                    value = row;
                }
                return value;
            });

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
        public async Task Execute_QuerySingleField_ReturnsSingleField()
        {
            // Given.
            // Create Ion struct to insert.
            IIonValue ionStruct = ValueFactory.NewEmptyStruct();
            ionStruct.SetField(Constants.ColumnName, ValueFactory.NewString(Constants.SingleDocumentValue));

            var query = $"INSERT INTO {Constants.TableName} ?";
            var count = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(query, ionStruct);

                var count = 0;
                await foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, count);

            // When.
            var searchQuery = $@"SELECT VALUE {Constants.ColumnName} FROM {Constants.TableName}
                                 WHERE {Constants.ColumnName} = '{Constants.SingleDocumentValue}'";
            var value = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(searchQuery);

                var value = "";
                await foreach (var row in result)
                {
                    value = row.StringValue;
                }
                return value;
            });

            // Then.
            Assert.AreEqual(Constants.SingleDocumentValue, value);
        }

        [TestMethod]
        public async Task Execute_QueryTableEnclosedInQuotes_ReturnsResult()
        {
            // Given.
            // Create Ion struct to insert.
            IIonValue ionStruct = ValueFactory.NewEmptyStruct();
            ionStruct.SetField(Constants.ColumnName, ValueFactory.NewString(Constants.SingleDocumentValue));

            var query = $"INSERT INTO {Constants.TableName} ?";
            var count = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(query, ionStruct);

                var count = 0;
                await foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, count);

            // When.
            var searchQuery = $@"SELECT VALUE {Constants.ColumnName} FROM ""{Constants.TableName}""
                                 WHERE {Constants.ColumnName} = '{Constants.SingleDocumentValue}'";
            var value = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(searchQuery);

                var value = "";
                await foreach (var row in result)
                {
                    value = row.StringValue;
                }
                return value;
            });

            // Then.
            Assert.AreEqual(Constants.SingleDocumentValue, value);
        }

        [TestMethod]
        public async Task Execute_InsertMultipleDocuments_DocumentsInserted()
        {
            IIonValue ionString1 = ValueFactory.NewString(Constants.MultipleDocumentValue1);
            IIonValue ionString2 = ValueFactory.NewString(Constants.MultipleDocumentValue2);

            // Given.
            // Create Ion structs to insert.
            IIonValue ionStruct1 = ValueFactory.NewEmptyStruct();
            ionStruct1.SetField(Constants.ColumnName, ionString1);

            IIonValue ionStruct2 = ValueFactory.NewEmptyStruct();
            ionStruct2.SetField(Constants.ColumnName, ionString2);

            List<IIonValue> parameters = new List<IIonValue>() { ionStruct1, ionStruct2 };

            // When.
            var query = $"INSERT INTO {Constants.TableName} <<?,?>>";
            var count = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(query, parameters);

                var count = 0;
                await foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(2, count);

            // Then.
            var searchQuery = $@"SELECT VALUE {Constants.ColumnName} FROM {Constants.TableName}
                                 WHERE {Constants.ColumnName} IN (?,?)";
            var values = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(searchQuery, ionString1, ionString2);

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
        public async Task Execute_DeleteSingleDocument_DocumentIsDeleted()
        {
            // Given.
            // Create Ion struct to insert.
            IIonValue ionStruct = ValueFactory.NewEmptyStruct();
            ionStruct.SetField(Constants.ColumnName, ValueFactory.NewString(Constants.SingleDocumentValue));

            var query = $"INSERT INTO {Constants.TableName} ?";
            var count = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(query, ionStruct);

                var count = 0;
                await foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, count);

            // When.
            var deleteQuery = $@"DELETE FROM { Constants.TableName}
                                 WHERE {Constants.ColumnName} = '{Constants.SingleDocumentValue}'";
            var deletedCount = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(deleteQuery);

                var count = 0;
                await foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, deletedCount);

            // Then.
            var searchQuery = $"SELECT COUNT(*) FROM {Constants.TableName}";
            var searchCount = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(searchQuery);

                int count = -1;
                await foreach (var row in result)
                {
                    // This gives:
                    // {
                    //    _1: 1
                    // }
                    IIonValue ionValue = row.GetField("_1");
                    count = ((IIonInt)ionValue).IntValue;
                }
                return count;
            });
            Assert.AreEqual(0, searchCount);
        }

        [TestMethod]
        public async Task Execute_DeleteAllDocuments_DocumentsAreDeleted()
        {
            // Given.
            // Create Ion structs to insert.
            IIonValue ionStruct1 = ValueFactory.NewEmptyStruct();
            ionStruct1.SetField(Constants.ColumnName, ValueFactory.NewString(Constants.MultipleDocumentValue1));

            IIonValue ionStruct2 = ValueFactory.NewEmptyStruct();
            ionStruct2.SetField(Constants.ColumnName, ValueFactory.NewString(Constants.MultipleDocumentValue2));

            List<IIonValue> parameters = new List<IIonValue>() { ionStruct1, ionStruct2 };

            var query = $"INSERT INTO {Constants.TableName} <<?,?>>";
            var count = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(query, parameters);

                var count = 0;
                await foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(2, count);

            // When.
            var deleteQuery = $"DELETE FROM { Constants.TableName}";
            var deleteCount = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(deleteQuery);

                var count = 0;
                await foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(2, deleteCount);

            // Then.
            var searchQuery = $"SELECT COUNT(*) FROM {Constants.TableName}";
            var searchCount = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(searchQuery);

                int count = -1;
                await foreach (var row in result)
                {
                    // This gives:
                    // {
                    //    _1: 1
                    // }
                    IIonValue ionValue = row.GetField("_1");
                    count = ((IIonInt)ionValue).IntValue;
                }
                return count;
            });
            Assert.AreEqual(0, searchCount);
        }

        [TestMethod]
        public async Task Execute_UpdateSameRecordAtSameTime_ThrowsOccException()
        {
            QldbDriver driver = integrationTestBase.CreateDriver(amazonQldbSessionConfig, default, default, 0);

            // Insert document.
            // Create Ion struct with int value 0 to insert.
            IIonValue ionStruct = ValueFactory.NewEmptyStruct();
            ionStruct.SetField(Constants.ColumnName, ValueFactory.NewInt(0));

            var query = $"INSERT INTO {Constants.TableName} ?";
            var count = await driver.Execute(async txn =>
            {
                var result = await txn.Execute(query, ionStruct);

                var count = 0;
                await foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, count);

            string selectQuery = $"SELECT VALUE {Constants.ColumnName} FROM {Constants.TableName}";
            string updateQuery = $"UPDATE {Constants.TableName} SET {Constants.ColumnName} = ?";

            try
            {
                // Run three threads updating the same document in parallel to trigger OCC exception.
                const int numThreads = 3;
                var tasks = new List<Task>();
                for (int i = 0; i < numThreads; i++)
                {
                    Func<Task> action = async () => await driver.Execute(async txn =>
                    {
                        // Query table.
                        var result = await txn.Execute(selectQuery);

                        var currentValue = 0;
                        await foreach (var row in result)
                        {
                            currentValue = row.IntValue;
                        }

                        // Update document.
                        var ionValue = ValueFactory.NewInt(currentValue + 5);
                        await txn.Execute(updateQuery, ionValue);
                    }, RetryPolicy.Builder().WithMaxRetries(0).Build());

                    tasks.Add(action());
                }

                await Task.WhenAll(tasks);
            }
            catch (OccConflictException e)
            {
                // Update document to make sure everything still works after the OCC exception.
                int updatedValue = 0;
                await driver.Execute(async txn =>
                {
                    var result = await txn.Execute(selectQuery);

                    var currentValue = 0;
                    await foreach (var row in result)
                    {
                        currentValue = row.IntValue;
                    }

                    updatedValue = currentValue + 5;

                    var ionValue = ValueFactory.NewInt(updatedValue);
                    await txn.Execute(updateQuery, ionValue);
                });

                // Verify the update was successful.
                int intVal = await driver.Execute(async txn =>
                {
                    var result = await txn.Execute(selectQuery);

                    int intValue = 0;
                    await foreach (var row in result)
                    {
                        intValue = row.IntValue;
                    }

                    return intValue;
                });

                Assert.AreEqual(updatedValue, intVal);

                return;
            }
            Assert.Fail("Did not raise TimeoutException.");
        }

        [TestMethod]
        [DynamicData(nameof(CreateIonValues), DynamicDataSourceType.Method)]
        public async Task Execute_InsertAndReadIonTypes_IonTypesAreInsertedAndRead(IIonValue ionValue)
        {
            // Given.
            // Create Ion struct to be inserted.
            IIonValue ionStruct = ValueFactory.NewEmptyStruct();
            ionStruct.SetField(Constants.ColumnName, ionValue);

            var query = $"INSERT INTO {Constants.TableName} ?";
            var insertCount = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(query, ionStruct);

                var count = 0;
                await foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, insertCount);

            // When.
            IIonValue searchResult;
            if (ionValue.IsNull)
            {
                var searchQuery = $@"SELECT VALUE { Constants.ColumnName } FROM { Constants.TableName }
                                     WHERE { Constants.ColumnName } IS NULL";
                searchResult = await qldbDriver.Execute(async txn =>
                {
                    var result = await txn.Execute(searchQuery);

                    IIonValue ionVal = null;
                    await foreach (var row in result)
                    {
                        ionVal = row;
                    }
                    return ionVal;
                });
            }
            else
            {
                var searchQuery = $@"SELECT VALUE { Constants.ColumnName } FROM { Constants.TableName }
                                     WHERE { Constants.ColumnName } = ?";
                searchResult = await qldbDriver.Execute(async txn =>
                {
                    var result = await txn.Execute(searchQuery, ionValue);

                    IIonValue ionVal = null;
                    await foreach (var row in result)
                    {
                        ionVal = row;
                    }
                    return ionVal;
                });
            }

            // Then.
            if (searchResult.Type() != ionValue.Type())
            {
                Assert.Fail($"The queried value type, { searchResult.Type().ToString() }," +
                    $"does not match { ionValue.Type().ToString() }.");
            }
        }

        [TestMethod]
        [DynamicData(nameof(CreateIonValues), DynamicDataSourceType.Method)]
        public async Task Execute_UpdateIonTypes_IonTypesAreUpdated(IIonValue ionValue)
        {
            // Given.
            // Create Ion struct to be inserted.
            IIonValue ionStruct = ValueFactory.NewEmptyStruct();
            ionStruct.SetField(Constants.ColumnName, ValueFactory.NewNull());

            // Insert first record which will be subsequently updated.
            var insertQuery = $"INSERT INTO {Constants.TableName} ?";
            var insertCount = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(insertQuery, ionStruct);

                var count = 0;
                await foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, insertCount);

            // When.
            var updateQuery = $"UPDATE { Constants.TableName } SET { Constants.ColumnName } = ?";
            var updateCount = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(updateQuery, ionValue);

                var count = 0;
                await foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, updateCount);

            // Then.
            IIonValue searchResult;
            if (ionValue.IsNull)
            {
                var searchQuery = $@"SELECT VALUE { Constants.ColumnName } FROM { Constants.TableName }
                                     WHERE { Constants.ColumnName } IS NULL";
                searchResult = await qldbDriver.Execute(async txn =>
                {
                    var result = await txn.Execute(searchQuery);

                    IIonValue ionVal = null;
                    await foreach (var row in result)
                    {
                        ionVal = row;
                    }
                    return ionVal;
                });
            }
            else
            {
                var searchQuery = $@"SELECT VALUE { Constants.ColumnName } FROM { Constants.TableName }
                                     WHERE { Constants.ColumnName } = ?";
                searchResult = await qldbDriver.Execute(async txn =>
                {
                    var result = await txn.Execute(searchQuery, ionValue);

                    IIonValue ionVal = null;
                    await foreach (var row in result)
                    {
                        ionVal = row;
                    }
                    return ionVal;
                });
            }

            if (searchResult.Type() != ionValue.Type())
            {
                Assert.Fail($"The queried value type, { searchResult.Type().ToString() }," +
                    $"does not match { ionValue.Type().ToString() }.");
            }
        }

        [TestMethod]
        public async Task Execute_ExecuteLambdaThatDoesNotReturnValue_RecordIsUpdated()
        {
            // Given.
            // Create Ion struct to insert.
            IIonValue ionStruct = ValueFactory.NewEmptyStruct();
            ionStruct.SetField(Constants.ColumnName, ValueFactory.NewString(Constants.SingleDocumentValue));

            // When.
            var query = $"INSERT INTO {Constants.TableName} ?";
            await qldbDriver.Execute(async txn => await txn.Execute(query, ionStruct));

            // Then.
            var searchQuery = $@"SELECT VALUE {Constants.ColumnName} FROM {Constants.TableName}
                                 WHERE {Constants.ColumnName} = '{Constants.SingleDocumentValue}'";
            var value = await qldbDriver.Execute(async txn =>
            {
                var result = await txn.Execute(searchQuery);

                string value = "";
                await foreach (var row in result)
                {
                    value = row.StringValue;
                }
                return value;
            });
            Assert.AreEqual(Constants.SingleDocumentValue, value);
        }

        [TestMethod]
        [ExpectedException(typeof(BadRequestException))]
        public async Task Execute_DeleteTableThatDoesntExist_ThrowsBadRequestException()
        {
            // Given.
            var query = "DELETE FROM NonExistentTable";

            // When.
            await qldbDriver.Execute(async txn => await txn.Execute(query));
        }

        public static IEnumerable<object[]> CreateIonValues()
        {
            var ionValues = new List<object[]>();

            var ionBlob = ValueFactory.NewBlob(Encoding.ASCII.GetBytes("value"));
            ionValues.Add(new object[] { ionBlob });

            var ionBool = ValueFactory.NewBool(true);
            ionValues.Add(new object[] { ionBool });

            var ionClob = ValueFactory.NewClob(Encoding.ASCII.GetBytes("{{ 'Clob value.'}}"));
            ionValues.Add(new object[] { ionClob });

            var ionDecimal = ValueFactory.NewDecimal(0.1);
            ionValues.Add(new object[] { ionDecimal });

            var ionFloat = ValueFactory.NewFloat(1.1);
            ionValues.Add(new object[] { ionFloat });

            var ionInt = ValueFactory.NewInt(2);
            ionValues.Add(new object[] { ionInt });

            var ionList = ValueFactory.NewEmptyList();
            ionList.Add(ValueFactory.NewInt(3));
            ionValues.Add(new object[] { ionList });

            var ionNull = ValueFactory.NewNull();
            ionValues.Add(new object[] { ionNull });

            var ionSexp = ValueFactory.NewEmptySexp();
            ionSexp.Add(ValueFactory.NewString("value"));
            ionValues.Add(new object[] { ionSexp });

            var ionString = ValueFactory.NewString("value");
            ionValues.Add(new object[] { ionString });

            var ionStruct = ValueFactory.NewEmptyStruct();
            ionStruct.SetField("value", ValueFactory.NewBool(true));
            ionValues.Add(new object[] { ionStruct });

            var ionSymbol = ValueFactory.NewSymbol("symbol");
            ionValues.Add(new object[] { ionSymbol });

            var ionTimestamp = ValueFactory.NewTimestamp(new IonDotnet.Timestamp(DateTime.Now));
            ionValues.Add(new object[] { ionTimestamp });

            var ionNullBlob = ValueFactory.NewNullBlob();
            ionValues.Add(new object[] { ionNullBlob });

            var ionNullBool = ValueFactory.NewNullBool();
            ionValues.Add(new object[] { ionNullBool });

            var ionNullClob = ValueFactory.NewNullClob();
            ionValues.Add(new object[] { ionNullClob });

            var ionNullDecimal = ValueFactory.NewNullDecimal();
            ionValues.Add(new object[] { ionNullDecimal });

            var ionNullFloat = ValueFactory.NewNullFloat();
            ionValues.Add(new object[] { ionNullFloat });

            var ionNullInt = ValueFactory.NewNullInt();
            ionValues.Add(new object[] { ionNullInt });

            var ionNullList = ValueFactory.NewNullList();
            ionValues.Add(new object[] { ionNullList });

            var ionNullSexp = ValueFactory.NewNullSexp();
            ionValues.Add(new object[] { ionNullSexp });

            var ionNullString = ValueFactory.NewNullString();
            ionValues.Add(new object[] { ionNullString });

            var ionNullStruct = ValueFactory.NewNullStruct();
            ionValues.Add(new object[] { ionNullStruct });

            var ionNullSymbol = ValueFactory.NewNullSymbol();
            ionValues.Add(new object[] { ionNullSymbol });

            var ionNullTimestamp = ValueFactory.NewNullTimestamp();
            ionValues.Add(new object[] { ionNullTimestamp });

            var ionBlobWithAnnotation = ValueFactory.NewBlob(Encoding.ASCII.GetBytes("value"));
            ionBlobWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionBlobWithAnnotation });

            var ionBoolWithAnnotation = ValueFactory.NewBool(true);
            ionBoolWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionBoolWithAnnotation });

            var ionClobWithAnnotation = ValueFactory.NewClob(Encoding.ASCII.GetBytes("{{ 'Clob value.'}}"));
            ionClobWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionClobWithAnnotation });

            var ionDecimalWithAnnotation = ValueFactory.NewDecimal(0.1);
            ionDecimalWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionDecimalWithAnnotation });

            var ionFloatWithAnnotation = ValueFactory.NewFloat(1.1);
            ionFloatWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionFloatWithAnnotation });

            var ionIntWithAnnotation = ValueFactory.NewInt(2);
            ionIntWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionIntWithAnnotation });

            var ionListWithAnnotation = ValueFactory.NewEmptyList();
            ionListWithAnnotation.Add(ValueFactory.NewInt(3));
            ionListWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionListWithAnnotation });

            var ionNullWithAnnotation = ValueFactory.NewNull();
            ionNullWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionNullWithAnnotation });

            var ionSexpWithAnnotation = ValueFactory.NewEmptySexp();
            ionSexpWithAnnotation.Add(ValueFactory.NewString("value"));
            ionSexpWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionSexpWithAnnotation });

            var ionStringWithAnnotation = ValueFactory.NewString("value");
            ionStringWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionStringWithAnnotation });

            var ionStructWithAnnotation = ValueFactory.NewEmptyStruct();
            ionStructWithAnnotation.SetField("value", ValueFactory.NewBool(true));
            ionStructWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionStructWithAnnotation });

            var ionSymbolWithAnnotation = ValueFactory.NewSymbol("symbol");
            ionSymbolWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionSymbolWithAnnotation });

            var ionTimestampWithAnnotation = ValueFactory.NewTimestamp(new IonDotnet.Timestamp(DateTime.Now));
            ionTimestampWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionTimestampWithAnnotation });

            var ionNullBlobWithAnnotation = ValueFactory.NewNullBlob();
            ionNullBlobWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionNullBlobWithAnnotation });

            var ionNullBoolWithAnnotation = ValueFactory.NewNullBool();
            ionNullBoolWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionNullBoolWithAnnotation });

            var ionNullClobWithAnnotation = ValueFactory.NewNullClob();
            ionNullClobWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionNullClobWithAnnotation });

            var ionNullDecimalWithAnnotation = ValueFactory.NewNullDecimal();
            ionNullDecimalWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionNullDecimalWithAnnotation });

            var ionNullFloatWithAnnotation = ValueFactory.NewNullFloat();
            ionNullFloatWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionNullFloatWithAnnotation });

            var ionNullIntWithAnnotation = ValueFactory.NewNullInt();
            ionNullIntWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionNullIntWithAnnotation });

            var ionNullListWithAnnotation = ValueFactory.NewNullList();
            ionNullListWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionNullListWithAnnotation });

            var ionNullSexpWithAnnotation = ValueFactory.NewNullSexp();
            ionNullSexpWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionNullSexpWithAnnotation });

            var ionNullStringWithAnnotation = ValueFactory.NewNullString();
            ionNullStringWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionNullStringWithAnnotation });

            var ionNullStructWithAnnotation = ValueFactory.NewNullStruct();
            ionNullStructWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionNullStructWithAnnotation });

            var ionNullSymbolWithAnnotation = ValueFactory.NewNullSymbol();
            ionNullSymbolWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionNullSymbolWithAnnotation });

            var ionNullTimestampWithAnnotation = ValueFactory.NewNullTimestamp();
            ionNullTimestampWithAnnotation.AddTypeAnnotation("annotation");
            ionValues.Add(new object[] { ionNullTimestampWithAnnotation });

            return ionValues;
        }
    }
}
