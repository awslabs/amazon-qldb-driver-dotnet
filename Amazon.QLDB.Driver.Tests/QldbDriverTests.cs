/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

namespace Amazon.QLDB.Driver.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Amazon.IonDotnet.Tree;
    using Microsoft.Extensions.Logging.Abstractions;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;
    using static Amazon.QLDB.Driver.QldbDriver;
    using System.Linq;
    using Amazon.IonDotnet.Tree.Impl;
    using Amazon.IonDotnet.Builders;
    using Amazon.IonDotnet.Utils;

    [TestClass]
    public class QldbDriverTests
    {
        private static QldbDriverBuilder builder;
        private static Mock<AmazonQLDBSessionClient> mockClient;
        private static readonly byte[] digest = new byte[] { 89, 49, 253, 196, 209, 176, 42, 98, 35, 214, 6, 163, 93,
            141, 170, 92, 75, 218, 111, 151, 173, 49, 57, 144, 227, 72, 215, 194, 186, 93, 85, 108,
        };

        [TestInitialize]
        public void SetupTest()
        {
            mockClient = new Mock<AmazonQLDBSessionClient>();
            var sendCommandResponse = new SendCommandResponse
            {
                StartSession = new StartSessionResult
                {
                    SessionToken = "testToken"
                },
                StartTransaction = new StartTransactionResult
                {
                    TransactionId = "testTransactionIdddddd"
                },
                ExecuteStatement = new ExecuteStatementResult
                {
                    FirstPage = new Page
                    {
                        NextPageToken = null,
                        Values = new List<ValueHolder>()
                    }
                },
                CommitTransaction = new CommitTransactionResult
                {
                    CommitDigest = new MemoryStream(digest)
                },
                ResponseMetadata = new ResponseMetadata
                {
                    RequestId = "testId"
                }
            };
            mockClient.Setup(x => x.SendCommandAsync(It.IsAny<SendCommandRequest>(), It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(sendCommandResponse));
            builder = QldbDriver.Builder().WithLedger("testLedger");
        }

        [TestMethod]
        public void TestBuilderGetsANotNullObject()
        {
            Assert.IsNotNull(builder);
        }

        [TestMethod]
        public void TestWithPoolLimitArgumentBounds()
        {
            QldbDriver driver;

            // Default pool limit
            driver = builder.Build();
            Assert.IsNotNull(driver);

            // Negative pool limit
            Assert.ThrowsException<ArgumentException>(() => builder.WithMaxConcurrentTransactions(-4));

            driver = builder.WithMaxConcurrentTransactions(0).Build();
            Assert.IsNotNull(driver);
            driver = builder.WithMaxConcurrentTransactions(4).Build();
            Assert.IsNotNull(driver);
        }

        [TestMethod]
        public void TestPooledQldbDriverConstructorReturnsValidObject()
        {
            var driver = new QldbDriver(
                new SessionPool(() => Session.StartSession("ledgerName", mockClient.Object, NullLogger.Instance), 4, 4, NullLogger.Instance));
            Assert.IsNotNull(driver);
        }
        
        [TestMethod]
        public void TestListTableNamesLists()
        {
            var factory = new ValueFactory();
            var tables = new List<string>() { "table1", "table2" };
            var ions = tables.Select(t => CreateValueHolder(factory.NewString(t))).ToList();

            var h1 = QldbHash.ToQldbHash("transactionId");
            h1 = Transaction.Dot(h1, QldbDriver.TableNameQuery, new List<IIonValue> { });
            
            var sendCommandResponseWithStartSession = new SendCommandResponse
            {
                StartSession = new StartSessionResult
                {
                    SessionToken = "testToken"
                },
                ResponseMetadata = new ResponseMetadata
                {
                    RequestId = "testId"
                }
            };
            var sendCommandResponseStartTransaction = new SendCommandResponse
            {
                StartTransaction = new StartTransactionResult
                {
                    TransactionId = "transactionId"
                },
                ResponseMetadata = new ResponseMetadata
                {
                    RequestId = "testId"
                }
            };
            var sendCommandResponseExecute = new SendCommandResponse
            {
                ExecuteStatement = new ExecuteStatementResult
                {
                    FirstPage = new Page
                    {
                        NextPageToken = null,
                        Values = ions
                    }
                },
                ResponseMetadata = new ResponseMetadata
                {
                    RequestId = "testId"
                }
            };
            var sendCommandResponseCommit = new SendCommandResponse
            {
                CommitTransaction = new CommitTransactionResult
                {
                    CommitDigest = new MemoryStream(h1.Hash),
                    TransactionId = "transactionId"
                },
                ResponseMetadata = new ResponseMetadata
                {
                    RequestId = "testId"
                }
            };

            mockClient.SetupSequence(x => x.SendCommandAsync(It.IsAny<SendCommandRequest>(), It.IsAny<CancellationToken>()))
                    .Returns(Task.FromResult(sendCommandResponseWithStartSession))
                    .Returns(Task.FromResult(sendCommandResponseStartTransaction))
                    .Returns(Task.FromResult(sendCommandResponseExecute))
                    .Returns(Task.FromResult(sendCommandResponseCommit));

            var driver = new QldbDriver(
                new SessionPool(() => Session.StartSession("ledgerName", mockClient.Object, NullLogger.Instance), 4, 4, NullLogger.Instance));

            var result = driver.ListTableNames();

            Assert.IsNotNull(result);
            CollectionAssert.AreEqual(tables, result.ToList());
        }

        [TestMethod]
        public void TestExecuteWithActionLambdaCanInvokeSuccessfully()
        {
            var driver = new QldbDriver(
                new SessionPool(() => Session.StartSession("ledgerName", mockClient.Object, NullLogger.Instance), 4, 4, NullLogger.Instance));
            driver.Execute((txn) =>
            {
                txn.Execute("testStatement");
            });
        }

        [TestMethod]
        public void TestExecuteWithActionLambdaAndRetryActionCanInvokeSuccessfully()
        {
            var driver = new QldbDriver(
                new SessionPool(() => Session.StartSession("ledgerName", mockClient.Object, NullLogger.Instance), 4, 4, NullLogger.Instance));
            driver.Execute((txn) =>
            {
                txn.Execute("testStatement");
            }, (int k) => { return; });
        }

        [TestMethod]
        public void TestExecuteWithFuncLambdaReturnsFuncOutput()
        {
            var driver = new QldbDriver(
                new SessionPool(() => Session.StartSession("ledgerName", mockClient.Object, NullLogger.Instance), 4, 4, NullLogger.Instance));
            var result = driver.Execute((txn) =>
            {
                txn.Execute("testStatement");
                return "testReturnValue";
            });
            Assert.AreEqual("testReturnValue", result);
        }

        [TestMethod]
        public void TestExecuteWithFuncLambdaAndRetryActionReturnsFuncOutput()
        {
            var driver = new QldbDriver(
                new SessionPool(() => Session.StartSession("ledgerName", mockClient.Object, NullLogger.Instance), 4, 4, NullLogger.Instance));
            driver.Dispose();
            Assert.ThrowsException<QldbDriverException>(() => driver.Execute((txn) =>
            {
                txn.Execute("testStatement");
                return "testReturnValue";
            }, (int k) => { return; }));
        }
        
        public static ValueHolder CreateValueHolder(IIonValue ionValue)
        {
            MemoryStream stream = new MemoryStream();
            using (var writer = IonBinaryWriterBuilder.Build(stream))
            {
                ionValue.WriteTo(writer);
                writer.Finish();
            }

            var valueHolder = new ValueHolder
            {
                IonBinary = new MemoryStream(stream.GetWrittenBuffer())
            };

            return valueHolder;
        }
    }
}
