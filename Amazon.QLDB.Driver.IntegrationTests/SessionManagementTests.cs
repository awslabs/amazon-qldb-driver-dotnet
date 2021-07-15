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
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;

    [TestClass]
    public class SessionManagementTests
    {
        private static AmazonQLDBSessionConfig amazonQldbSessionConfig;
        private static IntegrationTestBase integrationTestBase;

        [ClassInitialize]
        public static void SetUp(TestContext context)
        {
            // Get AWS configuration properties from .runsettings file.
            string region = context.Properties["region"].ToString();
            const string ledgerName = "DotnetSessionManagement";

            amazonQldbSessionConfig = IntegrationTestBase.CreateAmazonQLDBSessionConfig(region);
            integrationTestBase = new IntegrationTestBase(ledgerName, region);

            integrationTestBase.RunForceDeleteLedger();

            integrationTestBase.RunCreateLedger();
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            integrationTestBase.RunForceDeleteLedger();
        }

        [TestMethod]
        [ExpectedException(typeof(BadRequestException))]
        public void Connect_LedgerDoesNotExist_ThrowsBadRequestException()
        {
            using (var qldbDriver = integrationTestBase.CreateDriver(amazonQldbSessionConfig, null, 0, "NonExistentLedger"))
            {
                qldbDriver.ListTableNames();
            }
        }

        [TestMethod]
        public void GetSession_PoolDoesNotHaveSessionAndHasNotHitLimit_DoesNotThrowTimeoutException()
        {
            try
            {
                // Start a driver with default pool limit so it doesn't have sessions in the pool
                // and has not hit the limit.
                using (var qldbDriver = integrationTestBase.CreateDriver(amazonQldbSessionConfig))
                {
                    qldbDriver.ListTableNames();
                }
            }
            catch (TimeoutException e)
            {
                Assert.Fail(e.ToString());
            }
        }

        [TestMethod]
        public void GetSession_PoolHasSessionAndHasNotHitLimit_DoesNotThrowTimeoutException()
        {
            try
            {
                // Start a driver with default pool limit so it doesn't have sessions in the pool
                // and has not hit the limit.
                using (var qldbDriver = integrationTestBase.CreateDriver(amazonQldbSessionConfig))
                {
                    // Call the first ListTableNames() to start a session and put into pool.
                    qldbDriver.ListTableNames();

                    // Call the second ListTableNames() to use session from pool and is expected to execute successfully.
                    qldbDriver.ListTableNames();
                }
            }
            catch (TimeoutException e)
            {
                Assert.Fail(e.ToString());
            }
        }

        [TestMethod]
        [ExpectedException(typeof(QldbDriverException))]
        public void GetSession_PoolDoesNotHaveSessionAndHasHitLimit_ThrowsTimeoutException()
        {
            string TableNameQuery = "SELECT VALUE name FROM information_schema.user_tables WHERE status = 'ACTIVE'";

            // Create driver with session pool size = 1.
            var qldbDriver = integrationTestBase.CreateDriver(amazonQldbSessionConfig, null, 1);
            qldbDriver.Execute(txn =>
            {
                txn.Execute(TableNameQuery);

                // For testing purposes only. Forcefully fetch from the empty session pool.
                // Do not invoke QldbDriver.Execute within the lambda function under normal circumstances.
                qldbDriver.Execute(txn =>
                {
                    txn.Execute(TableNameQuery);
                });
            });
        }

        [TestMethod]
        [ExpectedException(typeof(QldbDriverException))]
        public void GetSession_DriverIsClosed_ThrowsObjectDisposedException()
        {
            using (var qldbDriver = integrationTestBase.CreateDriver(amazonQldbSessionConfig))
            {
                qldbDriver.Dispose();

                qldbDriver.ListTableNames();
            }
        }
    }
}
