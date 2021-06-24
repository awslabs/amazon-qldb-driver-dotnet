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

namespace Amazon.QLDB.Driver
{
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
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
        internal readonly IAmazonQLDBSession SessionClient;
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
            IAmazonQLDBSession sessionClient,
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
        /// Async factory method for constructing a new Session, creating a new session to QLDB on construction.
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
        internal static async Task<Session> StartSessionAsync(
            string ledgerName,
            IAmazonQLDBSession sessionClient,
            ILogger logger,
            CancellationToken cancellationToken)
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
        /// Factory method for constructing a new Session, creating a new session to QLDB on construction.
        /// </summary>
        ///
        /// <param name="ledgerName">The name of the ledger to create a session to.</param>
        /// <param name="sessionClient">The low-level session used for communication with QLDB.</param>
        /// <param name="logger">The logger to inject any logging framework.</param>
        ///
        /// <returns>A newly created <see cref="Session"/>.</returns>
        internal static Session StartSession(string ledgerName, IAmazonQLDBSession sessionClient, ILogger logger)
        {
            return
                StartSessionAsync(ledgerName, sessionClient, logger, CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Send an asynchronous abort request to QLDB, rolling back any active changes and closing any open results.
        /// </summary>
        ///
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// <returns>The result of the abort transaction request.</returns>
        internal virtual async Task<AbortTransactionResult> AbortTransactionAsync(CancellationToken cancellationToken)
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
        /// Send an abort request to QLDB, rolling back any active changes and closing any open results.
        /// </summary>
        ///
        /// <returns>The result of the abort transaction request.</returns>
        internal virtual AbortTransactionResult AbortTransaction()
        {
            return this.AbortTransactionAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Send an asynchronous end session request to QLDB and ignore exceptions.
        /// </summary>
        internal virtual async void End()
        {
            try
            {
                await this.EndSessionAsync(CancellationToken.None);
            }
            catch (AmazonServiceException ase)
            {
                this.logger.LogWarning("Error disposing session: {}", ase.Message);
            }
        }

        /// <summary>
        /// Send an asynchronous end session request to QLDB, closing all open results and transactions.
        /// </summary>
        ///
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// <returns>The result of the end session request.</returns>
        internal virtual async Task<EndSessionResult> EndSessionAsync(CancellationToken cancellationToken)
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
        /// Send an end session request to QLDB, closing all open results and transactions.
        /// </summary>
        ///
        /// <returns>The result of the end session request.</returns>
        internal virtual EndSessionResult EndSession()
        {
            return this.EndSessionAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Send an asynchronous commit request to QLDB, committing any active changes and closing any open results.
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
        /// <exception cref="OccConflictException">
        /// Thrown if an OCC conflict has been detected within the transaction.
        /// </exception>
        internal virtual async Task<CommitTransactionResult> CommitTransactionAsync(
            string txnId,
            MemoryStream commitDigest,
            CancellationToken cancellationToken)
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
        /// Send a commit request to QLDB, committing any active changes and closing any open results.
        /// </summary>
        ///
        /// <param name="txnId">The unique ID of the transaction to commit.</param>
        /// <param name="commitDigest">The digest hash of the transaction to commit.</param>
        ///
        /// <returns>The result of the commit transaction request.</returns>
        ///
        /// <exception cref="OccConflictException">
        /// Thrown if an OCC conflict has been detected within the transaction.
        /// </exception>
        internal virtual CommitTransactionResult CommitTransaction(string txnId, MemoryStream commitDigest)
        {
            return this.CommitTransactionAsync(txnId, commitDigest, CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Send an asynchronous execute request with parameters to QLDB.
        /// </summary>
        ///
        /// <param name="txnId">The unique ID of the transaction to execute.</param>
        /// <param name="statement">The PartiQL statement to execute.</param>
        /// <param name="parameters">The parameters to use with the PartiQL statement for execution.</param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// <returns>
        /// The result of the execution, which contains a <see cref="Page"/> representing the first data chunk.
        /// </returns>
        internal virtual async Task<ExecuteStatementResult> ExecuteStatementAsync(
            string txnId, string statement, List<IIonValue> parameters, CancellationToken cancellationToken)
        {
            List<ValueHolder> valueHolders = null;

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

            return await this.ExecuteStatementAsync(txnId, statement, valueHolders.ToArray(), cancellationToken);
        }

        internal virtual async Task<ExecuteStatementResult> ExecuteStatementAsync(
            string txnId, string statement, ValueHolder[] parameters, CancellationToken cancellationToken)
        {
            try
            {
                var executeStatementRequest = new ExecuteStatementRequest
                {
                    TransactionId = txnId,
                    Statement = statement,
                    Parameters = parameters.ToList(),
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
                if (parameters != null && parameters.Length != 0)
                {
                    foreach (ValueHolder valueHolder in parameters)
                    {
                        valueHolder.IonBinary.Dispose();
                    }
                }
            }
        }

        /// <summary>
        /// Send an execute request with parameters to QLDB.
        /// </summary>
        ///
        /// <param name="txnId">The unique ID of the transaction to execute.</param>
        /// <param name="statement">The PartiQL statement to execute.</param>
        /// <param name="parameters">The parameters to use with the PartiQL statement for execution.</param>
        ///
        /// <returns>
        /// The result of the execution, which contains a <see cref="Page"/> representing the first data chunk.
        /// </returns>
        internal virtual ExecuteStatementResult ExecuteStatement(
            string txnId, string statement, List<IIonValue> parameters)
        {
            return this.ExecuteStatementAsync(txnId, statement, parameters, CancellationToken.None).GetAwaiter()
                .GetResult();
        }

        internal virtual ExecuteStatementResult ExecuteStatement(
            string txnId, string statement, ValueHolder[] parameters)
        {
            return this.ExecuteStatementAsync(txnId, statement, parameters, CancellationToken.None).GetAwaiter()
                .GetResult();
        }

        /// <summary>
        /// Send an asynchronous fetch result request to QLDB, retrieving the next chunk of data for the result.
        /// </summary>
        ///
        /// <param name="txnId">The unique ID of the transaction to execute.</param>
        /// <param name="nextPageToken">The token that indicates what the next expected page is.</param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// <returns>The result of the <see cref="FetchPageRequest"/>.</returns>
        internal virtual async Task<FetchPageResult> FetchPageAsync(
            string txnId,
            string nextPageToken,
            CancellationToken cancellationToken)
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
        /// Send a fetch result request to QLDB, retrieving the next chunk of data for the result.
        /// </summary>
        ///
        /// <param name="txnId">The unique ID of the transaction to execute.</param>
        /// <param name="nextPageToken">The token that indicates what the next expected page is.</param>
        ///
        /// <returns>The result of the <see cref="FetchPageRequest"/>.</returns>
        internal virtual FetchPageResult FetchPage(string txnId, string nextPageToken)
        {
            return this.FetchPageAsync(txnId, nextPageToken, CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Send an asynchronous start transaction request to QLDB.
        /// </summary>
        ///
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// <returns>The result of the start transaction request.</returns>
        internal virtual async Task<StartTransactionResult> StartTransactionAsync(
            CancellationToken cancellationToken)
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
        /// Send a start transaction request to QLDB.
        /// </summary>
        ///
        /// <returns>The result of the start transaction request.</returns>
        internal virtual StartTransactionResult StartTransaction()
        {
            return this.StartTransactionAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <summary>
        /// Send an asynchronous request to QLDB.
        /// </summary>
        ///
        /// <param name="request">The request to send.</param>
        /// <param name="cancellationToken">
        ///     A cancellation token that can be used by other objects or threads to receive notice of cancellation.
        /// </param>
        ///
        /// <returns>The result returned by QLDB for the request.</returns>
        private async Task<SendCommandResponse> SendCommand(
            SendCommandRequest request, CancellationToken cancellationToken)
        {
            request.SessionToken = this.sessionToken;
            this.logger.LogDebug("Sending request: {}", request);
            return await this.SessionClient.SendCommandAsync(request, cancellationToken);
        }
    }
}
