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

namespace Amazon.QLDB.Driver.AsyncIntegrationTests.utils
{
    using Amazon.QLDB.Model;
    using Amazon.QLDBSession;
    using NLog;
    using System.Threading;
    using Amazon.QLDB.Driver.IntegrationTests.utils;

    /// <summary>
    /// Helper class which provides functions that test QLDB directly and through the driver.
    /// </summary>
    internal class AsyncIntegrationTestBase
    {
        private static readonly Logger Logger = Logging.GetLogger();

        public string ledgerName;
        public string regionName;
        public AmazonQLDBClient amazonQldbClient;

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

        public AsyncIntegrationTestBase(string ledgerName, string regionName)
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

        public AsyncQldbDriver CreateDriver(
            AmazonQLDBSessionConfig amazonQldbSessionConfig,
            int maxConcurrentTransactions = default,
            string ledgerName = default,
            int retryLimit = -1)
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
}
