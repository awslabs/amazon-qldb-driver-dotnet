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

namespace Amazon.QLDB.Driver.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Net;
    using System.Reflection;
    using System.Threading.Tasks;
    using Amazon.IonDotnet.Builders;
    using Amazon.IonDotnet.Tree;
    using Amazon.IonDotnet.Utils;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Moq;

    internal static class TestingUtilities
    {
        internal static SendCommandResponse DefaultSendCommandResponse(
            string sessionToken,
            string transactionId,
            string requestId,
            byte[] digest)
        {
            return new SendCommandResponse
            {
                StartSession = new StartSessionResult
                {
                    SessionToken = sessionToken
                },
                StartTransaction = new StartTransactionResult
                {
                    TransactionId = transactionId
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
                    RequestId = requestId
                }
            };
        }
        
        internal static SendCommandResponse StartSessionResponse(string requestId)
        {
            return new SendCommandResponse
            {
                StartSession = new StartSessionResult { SessionToken = "testToken" },
                ResponseMetadata = new ResponseMetadata { RequestId = requestId }
            };
        }

        internal static SendCommandResponse StartTransactionResponse(string transactionId, string requestId)
        {
            return new SendCommandResponse
            {
                StartTransaction = new StartTransactionResult { TransactionId = transactionId },
                ResponseMetadata = new ResponseMetadata { RequestId = requestId }
            };
        }
        
        internal static SendCommandResponse ExecuteResponse(string requestId, List<ValueHolder> values)
        {
            Page firstPage;
            if (values == null)
            {
                firstPage = new Page {NextPageToken = null};
            }
            else
            {
                firstPage = new Page {NextPageToken = null, Values = values};
            }
            return new SendCommandResponse
            {
                ExecuteStatement = new ExecuteStatementResult { FirstPage = firstPage },
                ResponseMetadata = new ResponseMetadata { RequestId = requestId }
            };
        }
        
        internal static SendCommandResponse CommitResponse(string transactionId, string requestId, byte[] hash)
        {
            return new SendCommandResponse
            {
                CommitTransaction = new CommitTransactionResult
                {
                    CommitDigest = new MemoryStream(hash),
                    TransactionId = transactionId
                },
                ResponseMetadata = new ResponseMetadata { RequestId = requestId }
            };
        }

        internal static ValueHolder CreateValueHolder(IIonValue ionValue)
        {
            MemoryStream stream = new MemoryStream();
            using (var writer = IonBinaryWriterBuilder.Build(stream))
            {
                ionValue.WriteTo(writer);
                writer.Finish();
            }

            var valueHolder = new ValueHolder
            {
                IonBinary = new MemoryStream(stream.GetWrittenBuffer()),
            };

            return valueHolder;
        }

        internal static ExecuteStatementResult GetExecuteResultNullStats(List<ValueHolder> valueHolderList)
        {
            return new ExecuteStatementResult
            {
                FirstPage = new Page
                {
                    NextPageToken = "hasNextPage",
                    Values = valueHolderList,
                },
            };
        }

        internal static ExecuteStatementResult GetExecuteResultWithStats(
            List<ValueHolder> valueHolderList,
            IOUsage executeIO,
            TimingInformation executeTiming)
        {
            return new ExecuteStatementResult
            {
                FirstPage = new Page
                {
                    NextPageToken = "hasNextPage",
                    Values = valueHolderList,
                },
                ConsumedIOs = executeIO,
                TimingInformation = executeTiming,
            };
        }

        internal static FetchPageResult GetFetchResultNullStats(List<ValueHolder> valueHolderList)
        {
            return new FetchPageResult
            {
                Page = new Page { NextPageToken = null, Values = valueHolderList },
            };
        }

        internal static FetchPageResult GetFetchResultWithStats(
            List<ValueHolder> valueHolderList,
            IOUsage fetchIO,
            TimingInformation fetchTiming)
        {
            return new FetchPageResult
            {
                Page = new Page { NextPageToken = null, Values = valueHolderList },
                ConsumedIOs = fetchIO,
                TimingInformation = fetchTiming,
            };
        }

        internal class ExecuteExceptionTestHelperAttribute : Attribute, ITestDataSource
        {
            public IEnumerable<object[]> GetData(MethodInfo methodInfo)
            {
                return new List<object[]>() {
                new object[] { new CapacityExceededException("Capacity Exceeded Exception", ErrorType.Receiver, "errorCode", "requestId", HttpStatusCode.ServiceUnavailable),
                    typeof(RetriableException), typeof(CapacityExceededException),
                    Times.Once()},
                new object[] { new AmazonQLDBSessionException("", 0, "", "", HttpStatusCode.InternalServerError),
                    typeof(RetriableException), null,
                    Times.Once()},
                new object[] { new AmazonQLDBSessionException("", 0, "", "", HttpStatusCode.ServiceUnavailable),
                    typeof(RetriableException), null,
                    Times.Once()},
                new object[] { new AmazonQLDBSessionException("", 0, "", "", HttpStatusCode.Unauthorized),
                    typeof(QldbTransactionException), null,
                    Times.Once()},
                new object[] { new OccConflictException("occ"),
                    typeof(RetriableException), typeof(OccConflictException),
                    Times.Never()},
                new object[] { new AmazonServiceException(),
                    typeof(QldbTransactionException), typeof(AmazonServiceException),
                    Times.Once()},
                new object[] { new InvalidSessionException("invalid session"),
                    typeof(RetriableException), typeof(InvalidSessionException),
                    Times.Never()},
                new object[] { new QldbTransactionException(string.Empty, true, new BadRequestException("Bad request")),
                    typeof(QldbTransactionException), typeof(QldbTransactionException),
                    Times.Once()},
                new object[] { new TransactionAbortedException("testTransactionIdddddd", true),
                    typeof(TransactionAbortedException), null,
                    Times.Never()},
                new object[] { new Exception("Customer Exception"),
                    typeof(QldbTransactionException), typeof(Exception),
                    Times.Once()}
                };
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

        internal class CreateExceptionsAttribute : Attribute, ITestDataSource
        {
            public IEnumerable<object[]> GetData(MethodInfo methodInfo)
            {
                return new List<object[]>()
                {
                    new object[] { new InvalidSessionException("message") },
                    new object[] { new OccConflictException("message") },
                    new object[] { new AmazonServiceException("message", new Exception(), HttpStatusCode.InternalServerError) },
                    new object[] { new AmazonServiceException("message", new Exception(), HttpStatusCode.ServiceUnavailable) },
                    new object[] { new AmazonServiceException("message", new Exception(), HttpStatusCode.Conflict) },
                    new object[] { new Exception("message")},
                    new object[] { new CapacityExceededException("message", ErrorType.Receiver, "errorCode", "requestId", HttpStatusCode.ServiceUnavailable) },
                };
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
    }
}
