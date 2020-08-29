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
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

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

            amazonQldbSessionConfig = IntegrationTestBase.CreateAmazonQLDBSessionConfig(region);
            integrationTestBase = new IntegrationTestBase(Constants.LedgerName, region);

            integrationTestBase.RunForceDeleteLedger();

            integrationTestBase.RunCreateLedger();
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            integrationTestBase.RunDeleteLedger();
        }

        [TestMethod]
        [ExpectedException(typeof(BadRequestException))]
        public async Task Connect_LedgerDoesNotExist_ThrowsBadRequestException()
        {
            await using (var qldbDriver = integrationTestBase.CreateDriver(amazonQldbSessionConfig, 0, "NonExistentLedger"))
            {
                await qldbDriver.ListTableNames();
            }
        }

        [TestMethod]
        public async Task GetSession_PoolDoesNotHaveSessionAndHasNotHitLimit_DoesNotThrowTimeoutException()
        {
            try
            {
                // Start a driver with default pool limit so it doesn't have sessions in the pool
                // and has not hit the limit.
                await using (var qldbDriver = integrationTestBase.CreateDriver(amazonQldbSessionConfig))
                {
                    await qldbDriver.ListTableNames();
                }
            }
            catch (TimeoutException e)
            {
                Assert.Fail(e.ToString());
            }
        }

        [TestMethod]
        public async Task GetSession_PoolHasSessionAndHasNotHitLimit_DoesNotThrowTimeoutException()
        {
            try
            {
                // Start a driver with default pool limit so it doesn't have sessions in the pool
                // and has not hit the limit.
                await using (var qldbDriver = integrationTestBase.CreateDriver(amazonQldbSessionConfig))
                {
                    // Call the first ListTableNames() to start a session and put into pool.
                    await qldbDriver.ListTableNames();

                    // Call the second ListTableName() to use session from pool and is expected to execute successfully.
                    await qldbDriver.ListTableNames();
                }
            }
            catch (TimeoutException e)
            {
                Assert.Fail(e.ToString());
            }
        }

        [TestMethod]
        public async Task GetSession_PoolDoesNotHaveSessionAndHasHitLimit_ThrowsTimeoutException()
        {
            try
            {
                // With the poolTimeout to just 1 ms, only one thread should go through.
                // The other two threads will try to acquire the session, but because it can wait for only 1ms,
                // they will error out.
                await using (var qldbDriver = integrationTestBase.CreateDriver(amazonQldbSessionConfig, 1))
                {
                    const int numThreads = 3;
                    List<Task> tasks = new List<Task>();

                    for (int i = 0; i < numThreads; i++)
                    {
                        Task task = new Task(() => qldbDriver.ListTableNames().GetAwaiter().GetResult());

                        tasks.Add(task);
                    }

                    foreach (Task task in tasks)
                    {
                        task.Start();
                    }

                    foreach (Task task in tasks)
                    {
                        task.Wait();
                    }
                }
            }
            catch (AggregateException e)
            {
                // Tasks only throw AggregateException which nests the underlying exeption.
                Assert.AreEqual(e.InnerException.GetType(), typeof(QldbDriverException));
                return;
            }
            Assert.Fail("Did not raise TimeoutException.");
        }

        [TestMethod]
        [ExpectedException(typeof(QldbDriverException))]
        public async Task GetSession_DriverIsClosed_ThrowsObjectDisposedException()
        {
            await using (var qldbDriver = integrationTestBase.CreateDriver(amazonQldbSessionConfig))
            {
                await qldbDriver.DisposeAsync();

                await qldbDriver.ListTableNames();
            }
        }
    }
}
