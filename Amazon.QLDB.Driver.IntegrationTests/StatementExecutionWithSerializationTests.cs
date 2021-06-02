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
    using System.IO;

    [TestClass]
    public class StatementExecutionWithSerializationTests
    {
        private static readonly IValueFactory ValueFactory = new ValueFactory();
        private static AmazonQLDBSessionConfig amazonQldbSessionConfig;
        private static IntegrationTestBase integrationTestBase;
        private static QldbDriver qldbDriver;
        private static string ledgerName = "DotnetStatementExecution";

        [ClassInitialize]
        public static void SetUp(TestContext context)
        {
            // Get AWS configuration properties from .runsettings file.
            string region = "us-east-1";

            amazonQldbSessionConfig = IntegrationTestBase.CreateAmazonQLDBSessionConfig(region);
            integrationTestBase = new IntegrationTestBase(ledgerName, region);

            integrationTestBase.RunForceDeleteLedger();

            integrationTestBase.RunCreateLedger();
            qldbDriver = CreateDriver(amazonQldbSessionConfig);

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

        class ParameterObject 
        {
            string Name
            {
                get 
                {
                    return Constants.SingleDocumentValue;
                }
            }
        }

        class ResultObject 
        {
            string DocumentId
            {
                get; set;
            }
        }

        [TestMethod]
        public void Execute_InsertDocument_UsingObjectSerialization()
        {
            // Given.
            // Create Ion struct to insert.
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

        public static QldbDriver CreateDriver(
            AmazonQLDBSessionConfig amazonQldbSessionConfig,
            int maxConcurrentTransactions = default,
            string ledgerName = default)
        {
            QldbDriverBuilder builder = QldbDriver.Builder();

            string finalLedgerName;

            if (ledgerName != default)
            {
                finalLedgerName = ledgerName;
            }
            else
            {
                finalLedgerName = ledgerName;
            }

            if (maxConcurrentTransactions != default)
            {
                builder.WithMaxConcurrentTransactions(maxConcurrentTransactions);
            }

            return builder.WithQLDBSessionConfig(amazonQldbSessionConfig)
                .WithLedger("DotnetStatementExecution")
                .WithSerializer(new MySerialization())
                .Build();
        }

        class MySerialization : ISerializer
        {
            public T Deserialize<T>(ValueHolder v)
            {
                return (T)(object)new ResultObject();
            }

            public ValueHolder Serialize(object o)
            {
                IIonValue ionValue = ValueFactory.NewEmptyStruct();
                ionValue.SetField(Constants.ColumnName, ValueFactory.NewString(Constants.SingleDocumentValue));
                var stream = new MemoryStream();
                using (var writer = IonBinaryWriterBuilder.Build(stream))
                {
                    ionValue.WriteTo(writer);
                    writer.Finish();
                }
                stream.Position = 0;
                return new ValueHolder { IonBinary = stream };
            }
        }
    }
}
