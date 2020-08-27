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

namespace Amazon.QLDB.Driver
{
    using System.Collections.Generic;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.IonDotnet.Builders;
    using Amazon.IonDotnet.Tree;
    using Amazon.QLDBSession;
    using Amazon.QLDBSession.Model;
    using Amazon.Runtime;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// Session object representing a connection with QLDB.
    /// </summary>
    internal class Session
    {
        internal readonly string LedgerName;
        internal readonly AmazonQLDBSessionClient SessionClient;
        internal readonly string SessionId;
        private readonly string sessionToken;
        private readonly ILogger logger;

        /// <summary>
        /// Initializes a new instance of the <see cref="Session"/> class to a specific ledger.
        /// </summary>
        ///
        /// <param name="ledgerName">The name of the ledger to create a session to.</param>
        /// <param name="sessionClient">The low-level session used for communication with QLDB.</param>
        /// <param name="sessionToken">The unique identifying token for this session to QLDB.</param>
        /// <param name="sessionId">The initial request ID for this session to QLDB.</param>
        /// <param name="logger">The logger to inject any logging framework.</param>
        internal Session(
            string ledgerName,
            AmazonQLDBSessionClient sessionClient,
            string sessionToken,
            string sessionId,
            ILogger logger)
        {
            this.LedgerName = ledgerName;
            this.SessionClient = sessionClient;
            this.sessionToken = sessionToken;
            this.SessionId = sessionId;
            this.logger = logger;
        }

        /// <summary>
        /// Factory method for constructing a new Session, creating a new session to QLDB on construction.
        /// </summary>
        ///
        /// <param name="ledgerName">The name of the ledger to create a session to.</param>
        /// <param name="sessionClient">The low-level session used for communication with QLDB.</param>
        /// <param name="logger">The logger to inject any logging framework.</param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// 
        /// <returns>A newly created <see cref="Session"/>.</returns>
        internal static async Task<Session> StartSession(string ledgerName, AmazonQLDBSessionClient sessionClient, ILogger logger, CancellationToken cancellationToken = default)
        {
            var startSessionRequest = new StartSessionRequest
            {
                LedgerName = ledgerName,
            };
            var request = new SendCommandRequest
            {
                StartSession = startSessionRequest,
            };

            logger.LogDebug("Sending start session request: {}", request);
            var response = await sessionClient.SendCommandAsync(request, cancellationToken);
            return new Session(
                ledgerName,
                sessionClient,
                response.StartSession.SessionToken,
                response.ResponseMetadata.RequestId,
                logger);
        }

        /// <summary>
        /// Send an abort request to QLDB, rolling back any active changes and closing any open results.
        /// </summary>
        ///
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// <returns>The result of the abort transaction request.</returns>
        internal virtual async Task<AbortTransactionResult> AbortTransaction(CancellationToken cancellationToken = default)
        {
            var abortTransactionRequest = new AbortTransactionRequest();
            var request = new SendCommandRequest
            {
                AbortTransaction = abortTransactionRequest,
            };
            var response = await this.SendCommand(request, cancellationToken);
            return response.AbortTransaction;
        }

        /// <summary>
        /// Send an end session request to QLDB and ignore exceptions.
        /// </summary>
        ///
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        internal virtual async Task End(CancellationToken cancellationToken = default)
        {
            try
            {
                await this.EndSession(cancellationToken);
            }
            catch (AmazonServiceException ase)
            {
                this.logger.LogWarning("Error disposing session: {}", ase.Message);
            }
        }

        /// <summary>
        /// Send an end session request to QLDB, closing all open results and transactions.
        /// </summary>
        ///
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// <returns>The result of the end session request.</returns>
        internal virtual async Task<EndSessionResult> EndSession(CancellationToken cancellationToken = default)
        {
            var endSessionRequest = new EndSessionRequest();
            var request = new SendCommandRequest
            {
                EndSession = endSessionRequest,
            };
            var response = await this.SendCommand(request, cancellationToken);
            return response.EndSession;
        }

        /// <summary>
        /// Send a commit request to QLDB, committing any active changes and closing any open results.
        /// </summary>
        ///
        /// <param name="txnId">The unique ID of the transaction to commit.</param>
        /// <param name="commitDigest">The digest hash of the transaction to commit.</param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// <returns>The result of the commit transaction request.</returns>
        ///
        /// <exception cref="OccConflictException">Thrown if an OCC conflict has been detected within the transaction.</exception>
        internal virtual async Task<CommitTransactionResult> CommitTransaction(string txnId, MemoryStream commitDigest, CancellationToken cancellationToken = default)
        {
            var commitTransactionRequest = new CommitTransactionRequest
            {
                TransactionId = txnId,
                CommitDigest = commitDigest,
            };
            var request = new SendCommandRequest
            {
                CommitTransaction = commitTransactionRequest,
            };
            var response = await this.SendCommand(request, cancellationToken);
            return response.CommitTransaction;
        }

        /// <summary>
        /// Send an execute request with parameters to QLDB.
        /// </summary>
        ///
        /// <param name="txnId">The unique ID of the transaction to execute.</param>
        /// <param name="statement">The PartiQL statement to execute.</param>
        /// <param name="parameters">The parameters to use with the PartiQL statement for execution.</param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// 
        /// <returns>The result of the execution, which contains a <see cref="Page"/> representing the first data chunk.</returns>
        internal virtual async Task<ExecuteStatementResult> ExecuteStatement(string txnId, string statement, List<IIonValue> parameters, CancellationToken cancellationToken = default)
        {
            List<ValueHolder> valueHolders = null;

            try
            {
                valueHolders = parameters.ConvertAll(ionValue =>
                {
                    MemoryStream stream = new MemoryStream();
                    using (var writer = IonBinaryWriterBuilder.Build(stream))
                    {
                        ionValue.WriteTo(writer);
                        writer.Finish();
                    }

                    var valueHolder = new ValueHolder
                    {
                        IonBinary = stream,
                    };
                    return valueHolder;
                });

                var executeStatementRequest = new ExecuteStatementRequest
                {
                    TransactionId = txnId,
                    Statement = statement,
                    Parameters = valueHolders,
                };
                var request = new SendCommandRequest
                {
                    ExecuteStatement = executeStatementRequest,
                };
                var response = await this.SendCommand(request, cancellationToken);
                return response.ExecuteStatement;
            }
            catch (IOException e)
            {
                throw new QldbDriverException(ExceptionMessages.FailedToSerializeParameter + e.Message, e);
            }
            finally
            {
                if (valueHolders != null)
                {
                    valueHolders.ForEach(valueHolder =>
                    {
                        valueHolder.IonBinary.Dispose();
                    });
                }
            }
        }

        /// <summary>
        /// Send a fetch result request to QLDB, retrieving the next chunk of data for the result.
        /// </summary>
        ///
        /// <param name="txnId">The unique ID of the transaction to execute.</param>
        /// <param name="nextPageToken">The token that indicates what the next expected page is.</param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        /// 
        /// <returns>The result of the <see cref="FetchPageRequest"/>.</returns>
        internal virtual async Task<FetchPageResult> FetchPage(string txnId, string nextPageToken, CancellationToken cancellationToken = default)
        {
            var fetchPageRequest = new FetchPageRequest
            {
                TransactionId = txnId,
                NextPageToken = nextPageToken,
            };
            var request = new SendCommandRequest
            {
                FetchPage = fetchPageRequest,
            };
            var response = await this.SendCommand(request, cancellationToken);
            return response.FetchPage;
        }

        /// <summary>
        /// Send a start transaction request to QLDB.
        ///
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// </summary>
        ///
        /// <returns>The result of the start transaction request.</returns>
        internal virtual async Task<StartTransactionResult> StartTransaction(CancellationToken cancellationToken = default)
        {
            var startTransactionRequest = new StartTransactionRequest();
            var request = new SendCommandRequest
            {
                StartTransaction = startTransactionRequest,
            };
            var response = await this.SendCommand(request, cancellationToken);
            return response.StartTransaction;
        }

        /// <summary>
        /// Send a request to QLDB.
        /// </summary>
        ///
        /// <param name="request">The request to send.</param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// <returns>The result returned by QLDB for the request.</returns>
        private async Task<SendCommandResponse> SendCommand(SendCommandRequest request, CancellationToken cancellationToken = default)
        {
            request.SessionToken = this.sessionToken;
            this.logger.LogDebug("Sending request: {}", request);
            return await this.SessionClient.SendCommandAsync(request, cancellationToken);
        }
    }
}
