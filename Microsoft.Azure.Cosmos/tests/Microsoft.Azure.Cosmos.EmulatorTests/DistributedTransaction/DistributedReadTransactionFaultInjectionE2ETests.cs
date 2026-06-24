// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.SDK.EmulatorTests
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos;
    using Microsoft.Azure.Cosmos.FaultInjection;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using PartitionKey = Cosmos.PartitionKey;

    /// <summary>
    /// HYBRID fault-injection end-to-end tests for READ Distributed Transactions (DTX/DTC).
    ///
    /// The pattern these tests validate is the genuine fault-injection contract: a fault is
    /// injected at the transport layer (below <c>RetryHandler</c>) for the first N attempts via the
    /// FaultInjection <c>ChaosInterceptor</c>; once the rule's hit-limit (<see
    /// cref="FaultInjectionServerErrorResultBuilder.WithTimes(int)"/>) is exhausted the interceptor
    /// stops firing and the SDK's transport retry policy (<c>ClientRetryPolicy</c>) re-sends the
    /// request — which now flows through to the REAL transaction Coordinator and (for a retriable
    /// error) ultimately succeeds. In other words: MOCK the error, then let the RETRY go REAL.
    ///
    /// To run locally:
    ///     set COSMOS_DTX_ENDPOINT=https://your-account.documents.azure.com:443/
    ///     set COSMOS_DTX_KEY=your-master-key
    ///     dotnet test --filter "FullyQualifiedName~DistributedReadTransactionFaultInjectionE2ETests"
    ///
    /// This class runs in the "DistributedTransaction" test category and is NOT gated with
    /// [Ignore]. It requires the COSMOS_DTX_ENDPOINT / COSMOS_DTX_KEY environment variables
    /// pointing at a live DTX-enabled account (the public emulator does not implement
    /// /operations/dtc, and a live DTX-enabled Coordinator is required to observe the
    /// retry-then-real-success behaviour); without them the tests fail fast in TestInitialize.
    /// </summary>
    [TestClass]
    [DoNotParallelize]
    [TestCategory("DistributedTransaction")]
    public class DistributedReadTransactionFaultInjectionE2ETests
    {
        private const string DatabaseId = "DtxReadFaultInjectionE2ETestDb";
        private const string ContainerId = "DtxReadFaultInjectionE2ETestContainer";
        private const string PartitionKeyPath = "/pk";

        private CosmosClient bootstrapClient;
        private string endpoint;
        private string key;
        private Database database;
        private Container bootstrapContainer;

        [TestInitialize]
        public async Task TestInitialize()
        {
            this.endpoint = Environment.GetEnvironmentVariable("COSMOS_DTX_ENDPOINT");
            this.key = Environment.GetEnvironmentVariable("COSMOS_DTX_KEY");

            if (string.IsNullOrWhiteSpace(this.endpoint) || string.IsNullOrWhiteSpace(this.key))
            {
                Assert.Fail("COSMOS_DTX_ENDPOINT and COSMOS_DTX_KEY environment variables must be set.");
            }

            // A non-fault-injected bootstrap client used only to create the database/container and to
            // seed items. Each test builds its own fault-injected client from the same endpoint+key.
            this.bootstrapClient = new CosmosClient(
                this.endpoint,
                this.key,
                new CosmosClientOptions
                {
                    ConnectionMode = ConnectionMode.Gateway,
                    ConsistencyLevel = ConsistencyLevel.Session
                });

            this.database = (await this.bootstrapClient.CreateDatabaseIfNotExistsAsync(DatabaseId)).Database;
            this.bootstrapContainer = (await this.database.CreateContainerIfNotExistsAsync(
                new ContainerProperties(ContainerId, PartitionKeyPath))).Container;
        }

        [TestCleanup]
        public async Task TestCleanup()
        {
            if (this.bootstrapClient != null)
            {
                try
                {
                    await this.bootstrapClient.GetDatabase(DatabaseId).DeleteAsync();
                }
                catch { /* ignore */ }

                this.bootstrapClient.Dispose();
            }
        }

        // ─── Hybrid: inject N times → retry reaches the real Coordinator → success ──────────

        /// <summary>
        /// READ DTx hybrid: inject a single retriable server error on the
        /// <see cref="FaultInjectionOperationType.DistributedReadTransaction"/> request, then
        /// assert the SDK retried AND the read transaction ultimately returned 200 from the real
        /// Coordinator.
        /// </summary>
        [DataTestMethod]
        [DataRow(FaultInjectionServerErrorType.Gone)]
        [DataRow(FaultInjectionServerErrorType.ServiceUnavailable)]
        [DataRow(FaultInjectionServerErrorType.TooManyRequests)]
        [DataRow(FaultInjectionServerErrorType.Timeout)]
        public async Task ReadTransaction_RetriableServerError_RetriesThenSucceedsAgainstRealCoordinator(
            FaultInjectionServerErrorType errorType)
        {
            ToDoActivity doc = await this.SeedItemAsync(this.bootstrapContainer);

            string ruleId = $"dtx-read-{errorType}-{Guid.NewGuid():N}";
            FaultInjectionRule rule = new FaultInjectionRuleBuilder(
                id: ruleId,
                condition: new FaultInjectionConditionBuilder()
                    .WithConnectionType(FaultInjectionConnectionType.Gateway)
                    .WithOperationType(FaultInjectionOperationType.DistributedReadTransaction)
                    .Build(),
                result: FaultInjectionResultBuilder.GetResultBuilder(errorType)
                    .WithTimes(1)
                    .Build())
                .WithDuration(TimeSpan.FromMinutes(5))
                .Build();

            using CosmosClient fiClient = this.CreateFaultInjectedClient(rule, out _);
            Container fiContainer = fiClient.GetContainer(DatabaseId, ContainerId);

            DistributedTransactionResponse response = await fiClient
                .CreateDistributedReadTransaction()
                .ReadItem(fiContainer, new PartitionKey(doc.pk), doc.id)
                .CommitTransactionAsync(CancellationToken.None);

            Assert.IsTrue(
                rule.GetHitCount() >= 1,
                $"[{errorType}] FaultInjection rule should have been hit at least once. Hit count: {rule.GetHitCount()}.");

            Assert.AreEqual(
                HttpStatusCode.OK,
                response.StatusCode,
                $"[{errorType}] After the injected fault was exhausted, the retried read transaction " +
                $"should return 200 from the real Coordinator. Got: {response.StatusCode} (hit count {rule.GetHitCount()}).");
            Assert.IsTrue(response.Count > 0);
            Assert.AreEqual(
                HttpStatusCode.OK,
                response[0].StatusCode,
                $"[{errorType}] Per-op[0] should be 200 OK. Got: {response[0].StatusCode}.");

            response.Dispose();
        }

        // ─── Transport timeouts: a DTX hit by a response timeout is retried then succeeds ──────

        /// <summary>
        /// READ DTx hit by a CLIENT-PERCEIVED RESPONSE timeout: a ResponseDelay longer than the
        /// client's RequestTimeout makes the first read attempt give up waiting for the Coordinator
        /// response (a retriable 408); the idempotent retry then returns 200 from the real Coordinator.
        /// </summary>
        [TestMethod]
        public async Task ReadTransaction_ClientPerceivedResponseTimeout_RetriesThenSucceedsAgainstRealCoordinator()
        {
            ToDoActivity doc = await this.SeedItemAsync(this.bootstrapContainer);

            // Delay (20s) exceeds the client RequestTimeout (5s); WithTimes(1) faults a single
            // attempt. The request IS sent, but the read is idempotent so the retried read is safe.
            TimeSpan injectedDelay = TimeSpan.FromSeconds(20);
            TimeSpan requestTimeout = TimeSpan.FromSeconds(5);

            string ruleId = $"dtx-read-respto-{Guid.NewGuid():N}";
            FaultInjectionRule rule = new FaultInjectionRuleBuilder(
                id: ruleId,
                condition: new FaultInjectionConditionBuilder()
                    .WithConnectionType(FaultInjectionConnectionType.Gateway)
                    .WithOperationType(FaultInjectionOperationType.DistributedReadTransaction)
                    .Build(),
                result: FaultInjectionResultBuilder.GetResultBuilder(FaultInjectionServerErrorType.ResponseDelay)
                    .WithDelay(injectedDelay)
                    .WithTimes(1)
                    .Build())
                .WithDuration(TimeSpan.FromMinutes(5))
                .Build();

            using CosmosClient fiClient = this.CreateFaultInjectedClient(rule, requestTimeout, out _);
            Container fiContainer = fiClient.GetContainer(DatabaseId, ContainerId);

            DistributedTransactionResponse response = await fiClient
                .CreateDistributedReadTransaction()
                .ReadItem(fiContainer, new PartitionKey(doc.pk), doc.id)
                .CommitTransactionAsync(CancellationToken.None);

            Assert.IsTrue(
                rule.GetHitCount() >= 1,
                $"The response timeout should have faulted at least one attempt. Hit count: {rule.GetHitCount()}.");
            Assert.AreEqual(
                HttpStatusCode.OK,
                response.StatusCode,
                $"After the injected response timeout was exhausted, the retried read transaction " +
                $"should return 200 from the real Coordinator. Got: {response.StatusCode} (hit count {rule.GetHitCount()}).");
            Assert.IsTrue(response.Count > 0);
            Assert.AreEqual(
                HttpStatusCode.OK,
                response[0].StatusCode,
                $"Per-op[0] should be 200 OK. Got: {response[0].StatusCode}.");

            response.Dispose();
        }

        // ─── Helpers ───────────────────────────────────────────────────────────────────────

        // Builds a CosmosClient (endpoint + master key, Gateway/Session) wired with the supplied
        // fault-injection rule so DTX requests are intercepted by the ChaosInterceptor.
        private CosmosClient CreateFaultInjectedClient(FaultInjectionRule rule, out FaultInjector faultInjector)
        {
            faultInjector = new FaultInjector(new List<FaultInjectionRule> { rule });

            CosmosClientOptions options = faultInjector.GetFaultInjectionClientOptions(
                new CosmosClientOptions
                {
                    ConnectionMode = ConnectionMode.Gateway,
                    ConsistencyLevel = ConsistencyLevel.Session
                });

            return new CosmosClient(this.endpoint, this.key, options);
        }

        // Overload: same as above but with an explicit client RequestTimeout, used by the transport-
        // timeout scenarios so an injected SendDelay/ResponseDelay longer than the timeout surfaces as
        // a client-perceived RequestTimeout (408).
        private CosmosClient CreateFaultInjectedClient(
            FaultInjectionRule rule,
            TimeSpan requestTimeout,
            out FaultInjector faultInjector)
        {
            faultInjector = new FaultInjector(new List<FaultInjectionRule> { rule });

            CosmosClientOptions options = faultInjector.GetFaultInjectionClientOptions(
                new CosmosClientOptions
                {
                    ConnectionMode = ConnectionMode.Gateway,
                    ConsistencyLevel = ConsistencyLevel.Session,
                    RequestTimeout = requestTimeout
                });

            return new CosmosClient(this.endpoint, this.key, options);
        }

        private async Task<ToDoActivity> SeedItemAsync(Container targetContainer)
        {
            ToDoActivity doc = ToDoActivity.CreateRandomToDoActivity();
            await targetContainer.CreateItemAsync(doc, new PartitionKey(doc.pk));
            return doc;
        }
    }
}
