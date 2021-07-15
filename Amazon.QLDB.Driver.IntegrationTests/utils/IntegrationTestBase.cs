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
    using Amazon.QLDB.Model;
    using Amazon.QLDBSession;
    using Amazon.IonDotnet.Builders;
    using Amazon.IonDotnet.Tree.Impl;
    using Amazon.IonDotnet.Tree;
    using NLog;
    using System;
    using System.Threading;
    using System.Collections.Generic;
    using System.Text;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System.Reflection;
    using System.Globalization;
    using System.IO;

    /// <summary>
    /// Helper class which provides functions that test QLDB directly and through the driver.
    /// </summary>
    internal class IntegrationTestBase
    {
        private static readonly Logger Logger = Logging.GetLogger();

        public string ledgerName;
        public string regionName;
        public AmazonQLDBClient amazonQldbClient;
        private static readonly IValueFactory ValueFactory = new ValueFactory();

        public static AmazonQLDBSessionConfig CreateAmazonQLDBSessionConfig(string region)
        {
            var amazonQLDBSessionConfig = new AmazonQLDBSessionConfig();

            if (region != null)
            {
                amazonQLDBSessionConfig.RegionEndpoint = RegionEndpoint.GetBySystemName(region);
            }
            return amazonQLDBSessionConfig;
        }

        public static AmazonQLDBConfig CreateAmazonQLDBConfig(string region)
        {
            var amazonQLDBConfig = new AmazonQLDBConfig();

            if (region != null)
            {
                amazonQLDBConfig.RegionEndpoint = RegionEndpoint.GetBySystemName(region);
            }
            return amazonQLDBConfig;
        }

        public IntegrationTestBase(string ledgerName, string regionName)
        {
            this.ledgerName = ledgerName;
            this.regionName = regionName;

            AmazonQLDBConfig amazonQldbConfig = CreateAmazonQLDBConfig(regionName);
            this.amazonQldbClient = new AmazonQLDBClient(amazonQldbConfig);
        }

        public void RunCreateLedger()
        {
            CreateLedgerRequest ledgerRequest = new CreateLedgerRequest
            {
                Name = this.ledgerName,
                PermissionsMode = PermissionsMode.ALLOW_ALL
            };

            this.amazonQldbClient.CreateLedgerAsync(ledgerRequest).GetAwaiter().GetResult();
            WaitForActive(this.ledgerName);
        }

        public QldbDriver CreateDriver(
            AmazonQLDBSessionConfig amazonQldbSessionConfig,
            ISerializer serializer = default,
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
                finalLedgerName = this.ledgerName;
            }

            if (maxConcurrentTransactions != default)
            {
                builder.WithMaxConcurrentTransactions(maxConcurrentTransactions);
            }

            return builder.WithQLDBSessionConfig(amazonQldbSessionConfig)
                .WithLedger(finalLedgerName)
                .WithSerializer(serializer)
                .Build();
        }

        public AsyncQldbDriver CreateAsyncDriver(
            AmazonQLDBSessionConfig amazonQldbSessionConfig,
            ISerializer serializer = default,
            int maxConcurrentTransactions = default,
            string ledgerName = default)
        {
            AsyncQldbDriverBuilder builder = AsyncQldbDriver.Builder();

            string finalLedgerName;

            if (ledgerName != default)
            {
                finalLedgerName = ledgerName;
            }
            else
            {
                finalLedgerName = this.ledgerName;
            }

            if (maxConcurrentTransactions != default)
            {
                builder.WithMaxConcurrentTransactions(maxConcurrentTransactions);
            }

            return builder.WithQLDBSessionConfig(amazonQldbSessionConfig)
                .WithLedger(finalLedgerName)
                .WithSerializer(serializer)
                .Build();
        }

        public void RunDeleteLedger()
        {
            UpdateLedgerDeletionProtection(this.ledgerName, false);
            var deleteRequest = new DeleteLedgerRequest
            {
                Name = this.ledgerName
            };

            this.amazonQldbClient.DeleteLedgerAsync(deleteRequest).GetAwaiter().GetResult();
            WaitForDeletion(this.ledgerName);
        }

        public void RunForceDeleteLedger()
        {
            try
            {
                UpdateLedgerDeletionProtection(this.ledgerName, false);
                var deleteRequest = new DeleteLedgerRequest
                {
                    Name = this.ledgerName
                };

                this.amazonQldbClient.DeleteLedgerAsync(deleteRequest).GetAwaiter().GetResult();
                WaitForDeletion(this.ledgerName);
            }
            catch (ResourceNotFoundException)
            {
                return;
            }
        }

        private DescribeLedgerResponse WaitForActive(string ledgerName)
        {
            var ledgerRequest = new DescribeLedgerRequest
            {
                Name = ledgerName
            };

            while (true)
            {
                var ledgerResponse = DescribeLedger(ledgerRequest);

                if (ledgerResponse.State.Equals(LedgerState.ACTIVE.Value))
                {
                    Logger.Info($"'{ ledgerName }' ledger created sucessfully.");
                    return ledgerResponse;
                }
                Logger.Info($"Creating the '{ ledgerName }' ledger...");
                Thread.Sleep(1000);
            }
        }

        private void UpdateLedgerDeletionProtection(string ledgerName, bool deletionProtection)
        {
            var ledgerRequest = new UpdateLedgerRequest
            {
                Name = ledgerName,
                DeletionProtection = deletionProtection
            };
            this.amazonQldbClient.UpdateLedgerAsync(ledgerRequest)
                .GetAwaiter()
                .GetResult();
        }

        private void WaitForDeletion(string ledgerName)
        {
            var ledgerRequest = new DescribeLedgerRequest { Name = ledgerName };

            while (true)
            {
                try
                {
                    var ledgerResponse = DescribeLedger(ledgerRequest);
                    Thread.Sleep(1000);
                }
                catch (ResourceNotFoundException)
                {
                    Logger.Info($"'{ ledgerName }' ledger deleted sucessfully.");
                    break;
                }
            }
        }

        private DescribeLedgerResponse DescribeLedger(DescribeLedgerRequest ledgerRequest)
        {
            return this.amazonQldbClient
                .DescribeLedgerAsync(ledgerRequest)
                .GetAwaiter()
                .GetResult();
        }
    }

    internal class CreateIonValuesAttribute : Attribute, ITestDataSource
    {
        private static readonly IValueFactory ValueFactory = new ValueFactory();
        public IEnumerable<object[]> GetData(MethodInfo methodInfo)
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

        public string GetDisplayName(MethodInfo methodInfo, object[] data)
        {
            if (data != null)
            {
                return string.Format(CultureInfo.CurrentCulture, "{0} ({1})", methodInfo.Name, string.Join(",", data));
            }
            return null;
        }
    }

    class ParameterObject
    {
        internal string Name
        {
            get
            {
                return Constants.SingleDocumentValue;
            }
        }

        public override string ToString()
        {
            return "<ParameterObject>{ Name: " + Name + " }";
        }
    }

    class ResultObject
    {
        internal string DocumentId
        {
            get; set;
        }

        public override string ToString()
        {
            return "<ResultObject>{ DocumentId: " + DocumentId + " }";
        }
    }
}
