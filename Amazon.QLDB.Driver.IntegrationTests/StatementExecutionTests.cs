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
    using Amazon.QLDB.Driver.Serialization;

    [TestClass]
    public class StatementExecutionTests
    {
        private static readonly IValueFactory ValueFactory = new ValueFactory();
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
            qldbDriver = integrationTestBase.CreateDriver(amazonQldbSessionConfig, new ObjectSerializer());

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
        public void Execute_DropExistingTable_TableDropped()
        {
            // Given.
            var create_table_query = $"CREATE TABLE {Constants.CreateTableName}";
            var create_table_count = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(create_table_query);

                var count = 0;
                foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, create_table_count);

            // Execute ListTableNames() to ensure table is created.
            var result = qldbDriver.ListTableNames();

            var tables = new List<string>();
            foreach (var row in result)
            {
                tables.Add(row);
            }
            Assert.IsTrue(tables.Contains(Constants.CreateTableName));

            // When.
            var drop_table_query = $"DROP TABLE {Constants.CreateTableName}";
            var drop_table_count = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(drop_table_query);

                var count = 0;
                foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, drop_table_count);

            // Then.
            tables.Clear();
            var updated_tables_result = qldbDriver.ListTableNames();
            foreach (var row in updated_tables_result)
            {
                tables.Add(row);
            }
            Assert.IsFalse(tables.Contains(Constants.CreateTableName));
        }

        [TestMethod]
        public void Execute_ListTables_ReturnsListOfTables()
        {
            // When.
            var result = qldbDriver.ListTableNames();

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
        [ExpectedException(typeof(BadRequestException))]
        public void Execute_CreateTableThatAlreadyExist_ThrowBadRequestException()
        {
            // Given.
            var query = $"CREATE TABLE {Constants.TableName}";

            // When.
            qldbDriver.Execute(txn => txn.Execute(query));
        }

        [TestMethod]
        public void Execute_CreateIndex_IndexIsCreated()
        {
            // Given.
            var query = $"CREATE INDEX on {Constants.TableName} ({Constants.IndexAttribute})";

            // When.
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

            // Then.
            var search_query = $@"SELECT VALUE indexes[0] FROM information_schema.user_tables
                                  WHERE status = 'ACTIVE' AND name = '{Constants.TableName}'";
            var indexColumn = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(search_query);

                // Extract the index name by querying the information_schema.
                /* This gives:
                {
                    expr: "[MyColumn]"
                }
                */
                var indexColumn = "";
                foreach (var row in result)
                {
                    indexColumn = row.GetField("expr").StringValue;
                }
                return indexColumn;
            });

            Assert.AreEqual("[" + Constants.IndexAttribute + "]", indexColumn);
        }

        [TestMethod]
        public void Execute_QueryTableThatHasNoRecords_ReturnsEmptyResult()
        {
            // Given.
            var query = $"SELECT * FROM {Constants.TableName}";

            // When.
            int resultSetSize = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(query);

                int count = 0;
                foreach (var row in result)
                {
                    count++;
                }
                return count;
            });

            // Then.
            Assert.AreEqual(0, resultSetSize);
        }

        [TestMethod]
        public void Execute_InsertDocument_DocumentIsInserted()
        {
            // Given.
            // Create Ion struct to insert.
            IIonValue ionStruct = ValueFactory.NewEmptyStruct();
            ionStruct.SetField(Constants.ColumnName, ValueFactory.NewString(Constants.SingleDocumentValue));

            // When.
            var query = $"INSERT INTO {Constants.TableName} ?";
            var count = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(query, ionStruct);

                var count = 0;
                foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, count);

            // Then.
            var searchQuery = $@"SELECT VALUE {Constants.ColumnName} FROM {Constants.TableName} 
                               WHERE {Constants.ColumnName} = '{Constants.SingleDocumentValue}'";
            var value = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(searchQuery);

                var value = "";
                foreach (var row in result)
                {
                    value = row.StringValue;
                }
                return value;
            });
            Assert.AreEqual(Constants.SingleDocumentValue, value);
        }

        [TestMethod]
        public void Execute_InsertDocument_UsingObjectSerialization()
        {
            ParameterObject testObject = new ParameterObject();

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

            var searchQuery = $"SELECT * FROM {Constants.TableName}";
            var searchResult = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(txn.Query<ParameterObject>(searchQuery));

                ParameterObject value = null;
                foreach (var row in result)
                {
                    value = row;
                }
                return value;
            });

            Assert.AreEqual(testObject.ToString(), searchResult.ToString());
        }

        [TestMethod]
        public void Execute_InsertDocumentWithMultipleFields_DocumentIsInserted()
        {
            // Given.
            // Create Ion struct to insert.
            IIonValue ionStruct = ValueFactory.NewEmptyStruct();
            ionStruct.SetField(Constants.ColumnName, ValueFactory.NewString(Constants.SingleDocumentValue));
            ionStruct.SetField(Constants.SecondColumnName, ValueFactory.NewString(Constants.SingleDocumentValue));

            // When.
            var query = $"INSERT  INTO {Constants.TableName} ?";
            var count = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(query, ionStruct);

                var count = 0;
                foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, count);

            // Then.
            var searchQuery = $@"SELECT {Constants.ColumnName}, {Constants.SecondColumnName} FROM {Constants.TableName} 
                               WHERE {Constants.ColumnName} = '{Constants.SingleDocumentValue}' AND  {Constants.SecondColumnName} = '{Constants.SingleDocumentValue}'";
            IIonValue value = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(searchQuery);

                IIonValue value = null;
                foreach (var row in result)
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
        public void Execute_QuerySingleField_ReturnsSingleField()
        {
            // Given.
            // Create Ion struct to insert.
            IIonValue ionStruct = ValueFactory.NewEmptyStruct();
            ionStruct.SetField(Constants.ColumnName, ValueFactory.NewString(Constants.SingleDocumentValue));

            var query = $"INSERT INTO {Constants.TableName} ?";
            var count = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(query, ionStruct);

                var count = 0;
                foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, count);

            // When.
            var searchQuery = $@"SELECT VALUE {Constants.ColumnName} FROM {Constants.TableName}
                                 WHERE {Constants.ColumnName} = '{Constants.SingleDocumentValue}'";
            var value = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(searchQuery);

                var value = "";
                foreach (var row in result)
                {
                    value = row.StringValue;
                }
                return value;
            });

            // Then.
            Assert.AreEqual(Constants.SingleDocumentValue, value);
        }

        [TestMethod]
        public void Execute_QueryTableEnclosedInQuotes_ReturnsResult()
        {
            // Given.
            // Create Ion struct to insert.
            IIonValue ionStruct = ValueFactory.NewEmptyStruct();
            ionStruct.SetField(Constants.ColumnName, ValueFactory.NewString(Constants.SingleDocumentValue));

            var query = $"INSERT INTO {Constants.TableName} ?";
            var count = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(query, ionStruct);

                var count = 0;
                foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, count);

            // When.
            var searchQuery = $@"SELECT VALUE {Constants.ColumnName} FROM ""{Constants.TableName}""
                                 WHERE {Constants.ColumnName} = '{Constants.SingleDocumentValue}'";
            var value = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(searchQuery);

                var value = "";
                foreach (var row in result)
                {
                    value = row.StringValue;
                }
                return value;
            });

            // Then.
            Assert.AreEqual(Constants.SingleDocumentValue, value);
        }

        [TestMethod]
        public void Execute_InsertMultipleDocuments_DocumentsInserted()
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
            var count = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(query, parameters);

                var count = 0;
                foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(2, count);

            // Then.
            var searchQuery = $@"SELECT VALUE {Constants.ColumnName} FROM {Constants.TableName}
                                 WHERE {Constants.ColumnName} IN (?,?)";
            var values = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(searchQuery, ionString1, ionString2);

                var values = new List<String>();
                foreach (var row in result)
                {
                    values.Add(row.StringValue);
                }
                return values;
            });
            Assert.IsTrue(values.Contains(Constants.MultipleDocumentValue1));
            Assert.IsTrue(values.Contains(Constants.MultipleDocumentValue2));
        }

        [TestMethod]
        public void Execute_DeleteSingleDocument_DocumentIsDeleted()
        {
            // Given.
            // Create Ion struct to insert.
            IIonValue ionStruct = ValueFactory.NewEmptyStruct();
            ionStruct.SetField(Constants.ColumnName, ValueFactory.NewString(Constants.SingleDocumentValue));

            var query = $"INSERT INTO {Constants.TableName} ?";
            var count = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(query, ionStruct);

                var count = 0;
                foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, count);

            // When.
            var deleteQuery = $@"DELETE FROM { Constants.TableName}
                                 WHERE {Constants.ColumnName} = '{Constants.SingleDocumentValue}'";
            var deletedCount = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(deleteQuery);

                var count = 0;
                foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, deletedCount);

            // Then.
            var searchQuery = $"SELECT COUNT(*) FROM {Constants.TableName}";
            var searchCount = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(searchQuery);

                int count = -1;
                foreach (var row in result)
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
        public void Execute_DeleteAllDocuments_DocumentsAreDeleted()
        {
            // Given.
            // Create Ion structs to insert.
            IIonValue ionStruct1 = ValueFactory.NewEmptyStruct();
            ionStruct1.SetField(Constants.ColumnName, ValueFactory.NewString(Constants.MultipleDocumentValue1));

            IIonValue ionStruct2 = ValueFactory.NewEmptyStruct();
            ionStruct2.SetField(Constants.ColumnName, ValueFactory.NewString(Constants.MultipleDocumentValue2));

            List<IIonValue> parameters = new List<IIonValue>() { ionStruct1, ionStruct2 };

            var query = $"INSERT INTO {Constants.TableName} <<?,?>>";
            var count = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(query, parameters);

                var count = 0;
                foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(2, count);

            // When.
            var deleteQuery = $"DELETE FROM { Constants.TableName}";
            var deleteCount = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(deleteQuery);

                var count = 0;
                foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(2, deleteCount);

            // Then.
            var searchQuery = $"SELECT COUNT(*) FROM {Constants.TableName}";
            var searchCount = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(searchQuery);

                int count = -1;
                foreach (var row in result)
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
        [ExpectedException(typeof(OccConflictException))]
        public void Execute_UpdateSameRecordAtSameTime_ThrowsOccException()
        {
            // Create a driver that does not retry OCC errors
            QldbDriver driver = integrationTestBase.CreateDriver(amazonQldbSessionConfig, default, default);

            // Insert document.
            // Create Ion struct with int value 0 to insert.
            IIonValue ionStruct = ValueFactory.NewEmptyStruct();
            ionStruct.SetField(Constants.ColumnName, ValueFactory.NewInt(0));

            var query = $"INSERT INTO {Constants.TableName} ?";
            var count = driver.Execute(txn =>
            {
                var result = txn.Execute(query, ionStruct);

                var count = 0;
                foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, count);

            string selectQuery = $"SELECT VALUE {Constants.ColumnName} FROM {Constants.TableName}";
            string updateQuery = $"UPDATE {Constants.TableName} SET {Constants.ColumnName} = ?";

            // For testing purposes only. Forcefully causes an OCC conflict to occur.
            // Do not invoke QldbDriver.Execute within the lambda function under normal circumstances.
            driver.Execute(txn =>
            {
                // Query table.
                var result = txn.Execute(selectQuery);

                var currentValue = 0;
                foreach (var row in result)
                {
                    currentValue = row.IntValue;
                }

                driver.Execute(txn =>
                {
                    // Update document.
                    var ionValue = ValueFactory.NewInt(currentValue + 5);
                    txn.Execute(updateQuery, ionValue);
                }, RetryPolicy.Builder().WithMaxRetries(0).Build());
            }, RetryPolicy.Builder().WithMaxRetries(0).Build());
        }

        [TestMethod]
        [CreateIonValues]
        public void Execute_InsertAndReadIonTypes_IonTypesAreInsertedAndRead(IIonValue ionValue)
        {
            // Given.
            // Create Ion struct to be inserted.
            IIonValue ionStruct = ValueFactory.NewEmptyStruct();
            ionStruct.SetField(Constants.ColumnName, ionValue);

            var query = $"INSERT INTO {Constants.TableName} ?";
            var insertCount = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(query, ionStruct);

                var count = 0;
                foreach (var row in result)
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
                searchResult = qldbDriver.Execute(txn =>
                {
                    var result = txn.Execute(searchQuery);

                    IIonValue ionVal = null;
                    foreach (var row in result)
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
                searchResult = qldbDriver.Execute(txn =>
                {
                    var result = txn.Execute(searchQuery, ionValue);

                    IIonValue ionVal = null;
                    foreach (var row in result)
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
        [CreateIonValues]
        public void Execute_UpdateIonTypes_IonTypesAreUpdated(IIonValue ionValue)
        {
            // Given.
            // Create Ion struct to be inserted.
            IIonValue ionStruct = ValueFactory.NewEmptyStruct();
            ionStruct.SetField(Constants.ColumnName, ValueFactory.NewNull());

            // Insert first record which will be subsequently updated.
            var insertQuery = $"INSERT INTO {Constants.TableName} ?";
            var insertCount = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(insertQuery, ionStruct);

                var count = 0;
                foreach (var row in result)
                {
                    count++;
                }
                return count;
            });
            Assert.AreEqual(1, insertCount);

            // When.
            var updateQuery = $"UPDATE { Constants.TableName } SET { Constants.ColumnName } = ?";
            var updateCount = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(updateQuery, ionValue);

                var count = 0;
                foreach (var row in result)
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
                searchResult = qldbDriver.Execute(txn =>
                {
                    var result = txn.Execute(searchQuery);

                    IIonValue ionVal = null;
                    foreach (var row in result)
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
                searchResult = qldbDriver.Execute(txn =>
                {
                    var result = txn.Execute(searchQuery, ionValue);

                    IIonValue ionVal = null;
                    foreach (var row in result)
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
        public void Execute_ExecuteLambdaThatDoesNotReturnValue_RecordIsUpdated()
        {
            // Given.
            // Create Ion struct to insert.
            IIonValue ionStruct = ValueFactory.NewEmptyStruct();
            ionStruct.SetField(Constants.ColumnName, ValueFactory.NewString(Constants.SingleDocumentValue));

            // When.
            var query = $"INSERT INTO {Constants.TableName} ?";
            qldbDriver.Execute(txn => txn.Execute(query, ionStruct));

            // Then.
            var searchQuery = $@"SELECT VALUE {Constants.ColumnName} FROM {Constants.TableName}
                                 WHERE {Constants.ColumnName} = '{Constants.SingleDocumentValue}'";
            var value = qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(searchQuery);

                string value = "";
                foreach (var row in result)
                {
                    value = row.StringValue;
                }
                return value;
            });
            Assert.AreEqual(Constants.SingleDocumentValue, value);
        }

        [TestMethod]
        [ExpectedException(typeof(BadRequestException))]
        public void Execute_DeleteTableThatDoesntExist_ThrowsBadRequestException()
        {
            // Given.
            var query = "DELETE FROM NonExistentTable";

            // When.
            qldbDriver.Execute(txn => txn.Execute(query));
        }

        [TestMethod]
        public void Execute_ExecutionMetrics()
        {
            qldbDriver.Execute(txn =>
            {
                var insertQuery = String.Format("INSERT INTO {0} << {{'col': 1}}, {{'col': 2}}, {{'col': 3}} >>",
                    Constants.TableName);
                txn.Execute(insertQuery);
            });

            // Given
            var selectQuery = String.Format("SELECT * FROM {0} as a, {0} as b, {0} as c, {0} as d, {0} as e, {0} as f",
                Constants.TableName);

            // When
            qldbDriver.Execute(txn =>
            {
                var result = txn.Execute(selectQuery);
                long readIOs = 0;
                long processingTime = 0;

                foreach (IIonValue row in result)
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
            var result = qldbDriver.Execute(txn =>
            {
                return txn.Execute(selectQuery);
            });

            var ioUsage = result.GetConsumedIOs();
            var timingInfo = result.GetTimingInformation();

            Assert.IsNotNull(ioUsage);
            Assert.IsNotNull(timingInfo);
            // The 1092 value is from selectQuery, that performs self joins on a table.
            Assert.AreEqual(1092, ioUsage?.ReadIOs);
            Assert.IsTrue(timingInfo?.ProcessingTimeMilliseconds > 0);
        }

        [TestMethod]
        public void Execute_ReturnTransactionIdAfterStatementExecution()
        {
            var query = $"SELECT * FROM {Constants.TableName}";
            var txnId = qldbDriver.Execute(txn =>
            {
                txn.Execute(query);

                return txn.Id;
            });
            
            Assert.IsNotNull(txnId);
            Assert.IsTrue(txnId.Length > 0);
        }
    }
}
