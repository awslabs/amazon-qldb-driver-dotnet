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
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;

    internal class MockSessionClient : IAmazonQLDBSession
    {
        private readonly Queue<Output> responses;
        private SendCommandResponse defaultResponse;

        internal MockSessionClient()
        {
            this.responses = new Queue<Output>();
            this.defaultResponse = new SendCommandResponse
            {
                StartSession = new StartSessionResult(),
                StartTransaction = new StartTransactionResult(),
                ExecuteStatement = new ExecuteStatementResult(),
                CommitTransaction = new CommitTransactionResult(),
                ResponseMetadata = new ResponseMetadata()
            };
        }

        // Not used
        public IClientConfig Config => throw new NotImplementedException();

        public void Dispose()
        {
            // Do nothing
        }

        internal void SetDefaultResponse(SendCommandResponse response)
        {
            this.defaultResponse = response;
        }

        public SendCommandResponse SendCommand(SendCommandRequest request)
        {
            // Not used
            throw new NotImplementedException();
        }

        public Task<SendCommandResponse> SendCommandAsync(SendCommandRequest request,
            CancellationToken cancellationToken = default)
        {
            if (responses.Count == 0)
            {
                return Task.FromResult(this.defaultResponse);
            }
            
            var output = responses.Dequeue();
            if (output.exception != null)
            {
                throw output.exception;
            }
            else
            {
                return Task.FromResult(output.response);
            }
        }

        internal void QueueResponse(SendCommandResponse response)
        {
            Output output = new Output
            {
                exception = null,
                response = response
            };

            this.responses.Enqueue(output);
        }

        internal void QueueResponse(Exception e)
        {
            Output output = new Output
            {
                exception = e,
                response = null
            };

            this.responses.Enqueue(output);
        }

        internal void Clear()
        {
            this.responses.Clear();
        }

        private struct Output
        {
            internal Exception exception;
            internal SendCommandResponse response;
        }
    }
}
