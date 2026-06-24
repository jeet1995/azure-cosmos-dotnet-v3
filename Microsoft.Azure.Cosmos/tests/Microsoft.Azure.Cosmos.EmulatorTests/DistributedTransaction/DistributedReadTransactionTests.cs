// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
// ------------------------------------------------------------

namespace Microsoft.Azure.Cosmos.SDK.EmulatorTests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Net;
    using System.Text;
    using System.Text.Json;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using PartitionKey = Cosmos.PartitionKey;

    /// <summary>
    /// Scenario tests for <see cref="DistributedReadTransaction"/>.
    ///
    /// These tests use a <see cref="DistributedTransactionMockHandler"/> to intercept the DTC
    /// commit request at the handler level while letting all other requests (container creation,
    /// RID resolution) flow to the real emulator. This lets us verify the full request/response
    /// cycle — serialization, response parsing, read result deserialization — without requiring the
    /// emulator to natively support distributed transactions.
    /// </summary>
    [TestClass]
    [DoNotParallelize]
    [TestCategory("DistributedTransaction")]
    public class DistributedReadTransactionTests : BaseCosmosClientHelper
    {
        private const string PartitionKeyPath = "/pk";

        private Container container;

        [TestInitialize]
        public async Task TestInitialize()
        {
            await this.TestInit();

            ContainerResponse containerResponse = await this.database.CreateContainerAsync(
                new ContainerProperties(id: Guid.NewGuid().ToString(), partitionKeyPath: PartitionKeyPath),
                cancellationToken: this.cancellationToken);

            this.container = containerResponse.Container;
        }

        [TestCleanup]
        public new async Task TestCleanup()
        {
            await base.TestCleanup();
        }

        // Read Transaction Tests

        [TestMethod]
        public async Task ValidateReadTransactionHappyPath()
        {
            // Arrange
            ToDoActivity doc1 = ToDoActivity.CreateRandomToDoActivity();
            ToDoActivity doc2 = ToDoActivity.CreateRandomToDoActivity();

            DistributedTransactionMockHandler handler = new DistributedTransactionMockHandler(
                request => Task.FromResult(this.BuildMockResponse(
                    HttpStatusCode.OK,
                    BuildReadSuccessResponseJson(2, JsonSerializer.Serialize(doc1), JsonSerializer.Serialize(doc2)))));

            using CosmosClient client = this.CreateMockClient(handler);

            // Act
            DistributedTransactionResponse response = await client
                .CreateDistributedReadTransaction()
                .ReadItem(this.GetContainerForClient(client, this.container), new PartitionKey(doc1.pk), doc1.id)
                .ReadItem(this.GetContainerForClient(client, this.container), new PartitionKey(doc2.pk), doc2.id)
                .CommitTransactionAsync(CancellationToken.None);

            // Assert
            Assert.IsNotNull(handler.CapturedRequestBody);
            Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
            Assert.IsTrue(response.IsSuccessStatusCode);
            Assert.AreEqual(2, response.Count);

            response.Dispose();
        }

        [TestMethod]
        public async Task ValidateReadTransactionRequestStructure()
        {
            // Arrange
            ToDoActivity doc = ToDoActivity.CreateRandomToDoActivity();

            DistributedTransactionMockHandler handler = new DistributedTransactionMockHandler(
                request => Task.FromResult(this.BuildMockResponse(
                    HttpStatusCode.OK,
                    BuildReadSuccessResponseJson(1, JsonSerializer.Serialize(doc)))));

            using CosmosClient client = this.CreateMockClient(handler);

            // Act
            DistributedTransactionResponse response = await client
                .CreateDistributedReadTransaction()
                .ReadItem(this.GetContainerForClient(client, this.container), new PartitionKey(doc.pk), doc.id)
                .CommitTransactionAsync(CancellationToken.None);

            // Assert – request structure
            Assert.IsNotNull(handler.CapturedRequestBody);
            using JsonDocument requestJson = JsonDocument.Parse(handler.CapturedRequestBody);
            JsonElement operation = requestJson.RootElement.GetProperty(DistributedTransactionSerializer.Operations)[0];

            Assert.AreEqual(OperationType.Read.ToString(), operation.GetProperty(DistributedTransactionSerializer.OperationType).GetString());
            Assert.AreEqual(doc.id, operation.GetProperty(DistributedTransactionSerializer.Id).GetString());
            Assert.IsTrue(operation.TryGetProperty(DistributedTransactionSerializer.DatabaseName, out _), "databaseName should be present");
            Assert.IsTrue(operation.TryGetProperty(DistributedTransactionSerializer.CollectionName, out _), "collectionName should be present");
            Assert.IsTrue(operation.TryGetProperty(DistributedTransactionSerializer.PartitionKey, out _), "partitionKey should be present");
            Assert.IsFalse(operation.TryGetProperty(DistributedTransactionSerializer.ResourceBody, out _), "resourceBody must NOT be present for read operations");

            response.Dispose();
        }

        [TestMethod]
        public async Task ValidateReadTransactionResponseDeserialization()
        {
            // Arrange
            ToDoActivity expectedDoc = ToDoActivity.CreateRandomToDoActivity();

            DistributedTransactionMockHandler handler = new DistributedTransactionMockHandler(
                request => Task.FromResult(this.BuildMockResponse(
                    HttpStatusCode.OK,
                    BuildReadSuccessResponseJson(1, JsonSerializer.Serialize(expectedDoc)))));

            using CosmosClient client = this.CreateMockClient(handler);

            // Act
            DistributedTransactionResponse response = await client
                .CreateDistributedReadTransaction()
                .ReadItem(this.GetContainerForClient(client, this.container), new PartitionKey(expectedDoc.pk), expectedDoc.id)
                .CommitTransactionAsync(CancellationToken.None);

            // Assert
            Assert.IsTrue(response.IsSuccessStatusCode);
            ToDoActivity actualDoc = JsonSerializer.Deserialize<ToDoActivity>(response[0].ResourceStream);
            Assert.IsNotNull(actualDoc);
            Assert.AreEqual(expectedDoc.id, actualDoc.id);
            Assert.AreEqual(expectedDoc.pk, actualDoc.pk);
            Assert.AreEqual(expectedDoc.taskNum, actualDoc.taskNum);

            response.Dispose();
        }

        [TestMethod]
        public async Task ValidateReadTransactionResourceStream()
        {
            // Arrange
            ToDoActivity expectedDoc = ToDoActivity.CreateRandomToDoActivity();

            DistributedTransactionMockHandler handler = new DistributedTransactionMockHandler(
                request => Task.FromResult(this.BuildMockResponse(
                    HttpStatusCode.OK,
                    BuildReadSuccessResponseJson(1, JsonSerializer.Serialize(expectedDoc)))));

            using CosmosClient client = this.CreateMockClient(handler);

            // Act
            DistributedTransactionResponse response = await client
                .CreateDistributedReadTransaction()
                .ReadItem(this.GetContainerForClient(client, this.container), new PartitionKey(expectedDoc.pk), expectedDoc.id)
                .CommitTransactionAsync(CancellationToken.None);

            // Assert – raw stream access
            Stream stream = response[0].ResourceStream;
            Assert.IsNotNull(stream);
            ToDoActivity actualDoc = JsonSerializer.Deserialize<ToDoActivity>(stream);
            Assert.AreEqual(expectedDoc.id, actualDoc.id);
            Assert.AreEqual(expectedDoc.pk, actualDoc.pk);

            response.Dispose();
        }

        [TestMethod]
        public void ValidateReadTransactionMissingIdThrows()
        {
            using CosmosClient client = this.CreateMockClient(new DistributedTransactionMockHandler(
                request => Task.FromResult(this.BuildMockResponse(HttpStatusCode.OK, BuildSuccessResponseJson(1)))));

            Assert.ThrowsException<ArgumentNullException>(() =>
                client.CreateDistributedReadTransaction()
                    .ReadItem(this.GetContainerForClient(client, this.container), new PartitionKey("pk"), id: null));
        }

        [TestMethod]
        public void ValidateReadTransactionMissingContainerThrows()
        {
            using CosmosClient client = this.CreateMockClient(new DistributedTransactionMockHandler(
                request => Task.FromResult(this.BuildMockResponse(HttpStatusCode.OK, BuildSuccessResponseJson(1)))));

            Assert.ThrowsException<ArgumentNullException>(() =>
                client.CreateDistributedReadTransaction()
                    .ReadItem(null, new PartitionKey("pk"), "item-id"));
        }

        // Helpers

        private Container GetContainerForClient(CosmosClient client, Container sourceContainer)
        {
            return client.GetContainer(sourceContainer.Database.Id, sourceContainer.Id);
        }

        private CosmosClient CreateMockClient(DistributedTransactionMockHandler handler)
        {
            return TestCommon.CreateCosmosClient(clientOptions: new CosmosClientOptions
            {
                CustomHandlers = { handler },
                ConnectionMode = ConnectionMode.Gateway
            });
        }

        private ResponseMessage BuildMockResponse(HttpStatusCode statusCode, string responseBody)
        {
            ResponseMessage response = new ResponseMessage(statusCode)
            {
                Content = new MemoryStream(Encoding.UTF8.GetBytes(responseBody))
            };
            response.Headers["x-ms-activity-id"] = Guid.NewGuid().ToString();
            return response;
        }

        private static string BuildSuccessResponseJson(int operationCount)
        {
            List<string> results = new List<string>();
            for (int i = 0; i < operationCount; i++)
            {
                results.Add($@"{{""index"":{i},""statusCode"":201,""etag"":""\""etag-{i}\""""}}");
            }

            return $@"{{""operationResponses"":[{string.Join(",", results)}]}}";
        }

        private static string BuildReadSuccessResponseJson(int operationCount, params string[] itemJsonBodies)
        {
            List<string> results = new List<string>();
            for (int i = 0; i < operationCount; i++)
            {
                string body = i < itemJsonBodies.Length ? itemJsonBodies[i] : "{}";
                results.Add($@"{{""index"":{i},""statusCode"":200,""etag"":""\""etag-{i}\"""",""resourceBody"":{body}}}");
            }

            return $@"{{""operationResponses"":[{string.Join(",", results)}]}}";
        }

        // Mock handler

        /// <summary>
        /// Intercepts DTC commit requests (URLs ending in "/dtc"), captures the serialized
        /// request body, and returns the response produced by the supplied factory. All other
        /// requests are forwarded to the next handler in the pipeline (the emulator).
        /// </summary>
        private class DistributedTransactionMockHandler : RequestHandler
        {
            private readonly Func<RequestMessage, Task<ResponseMessage>> mockResponseFactory;

            public string CapturedRequestBody { get; private set; }

            public DistributedTransactionMockHandler(Func<RequestMessage, Task<ResponseMessage>> mockResponseFactory)
            {
                this.mockResponseFactory = mockResponseFactory;
            }

            public override async Task<ResponseMessage> SendAsync(
                RequestMessage request,
                CancellationToken cancellationToken)
            {
                if (request.RequestUriString?.EndsWith("/dtc", StringComparison.OrdinalIgnoreCase) == true)
                {
                    string body = null;
                    if (request.Content != null)
                    {
                        using MemoryStream ms = new MemoryStream();
                        await request.Content.CopyToAsync(ms);
                        body = Encoding.UTF8.GetString(ms.ToArray());
                        request.Content.Position = 0;
                    }

                    this.CapturedRequestBody = body;

                    return await this.mockResponseFactory(request);
                }

                return await base.SendAsync(request, cancellationToken);
            }
        }
    }
}
