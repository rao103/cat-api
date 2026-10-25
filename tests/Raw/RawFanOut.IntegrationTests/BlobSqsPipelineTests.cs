using Amazon.Runtime;
using Amazon.SQS;
using Amazon.SQS.Model;
using Azure.Storage;
using Azure.Storage.Blobs;
using CAT.Messaging.Contracts;
using CAT.RawFanOut.Adapters.Messaging;
using CAT.RawFanOut.Adapters.Parquet;
using CAT.RawFanOut.Adapters.Storage;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Models;
using CAT.RawFanOut.Operations.Delivery;
using CAT.RawFanOut.Operations.FanOut;
using CAT.RawFanOut.Repositories;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
namespace CAT.RawFanOut.IntegrationTests;

public sealed class BlobSqsPipelineTests
{
    [ExternalIntegrationFact] public async Task RealBlobParquetToSqsRoundTripPreservesIdentityAndAcknowledgesParent()
    {
        // These credentials and endpoint are ONLY for the isolated emulators in compose.integration.yml.
        var account="fanouttest";
        var key=Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("01234567890123456789012345678901"));
        var blobService=new BlobServiceClient(new Uri("http://127.0.0.1:10000/"+account),new StorageSharedKeyCredential(account,key));
        var container=blobService.GetBlobContainerClient("fanout-"+Guid.NewGuid().ToString("N"));
        using var sqs=new AmazonSQSClient(new BasicAWSCredentials("test","test"),
            new AmazonSQSConfig{ServiceURL="http://127.0.0.1:4566",AuthenticationRegion="eu-north-1"});
        var input=(await sqs.CreateQueueAsync("raw-"+Guid.NewGuid().ToString("N"))).QueueUrl;
        var output=(await sqs.CreateQueueAsync("harmonize-"+Guid.NewGuid().ToString("N"))).QueueUrl;
        try
        {
            await container.CreateAsync();
            var bytes=await ParquetFixtures.WriteAsync("Production",["T1","T2"],new[]{
                new DateTime(2026,6,3,0,0,0,DateTimeKind.Utc),new DateTime(2026,6,3,0,10,0,DateTimeKind.Utc)});
            await container.GetBlobClient("raw/a.parquet").UploadAsync(new BinaryData(bytes));
            await sqs.SendMessageAsync(input,ParquetFixtures.Body("Production",["raw/a.parquet"]));
            var options=new FanOutOptions{VisibilitySeconds=3,HeartbeatSeconds=1};
            var reader=new ParquetIdentityReader(new AzureBlobObjectStore(container,options),options);
            await using var database = await PostgresTests.OpenAsync();
            // The farm catalog tables belong to the wider control plane, so seed just what the planner reads.
            await using (var seed = database.CreateCommand("""
                INSERT INTO windfarm(id, windfarm_name) VALUES (7, 'WF07');
                INSERT INTO wind_turbine(id, wtg_name, windfarm_id) VALUES (101, 'T1', 7), (102, 'T2', 7);
                """))
                await seed.ExecuteNonQueryAsync();

            var processor = new RawFanOutProcessor(
                new FanOutPlanner(reader, new PostgresFarmCatalog(new TestPostgresConnectionFactory(database)), options,
                    TestSupport.DefaultStrategies(options), new NullTelemetry<FanOutPlanner>()),
                new ReliableBatchPublisher(
                    new SqsBatchTransport(sqs, output),
                    options,
                    new NullTelemetry<ReliableBatchPublisher>()),
                options,
                new PostgresArtifactDispatchStore(new TestPostgresConnectionFactory(database)),
                new NullTelemetry<RawFanOutProcessor>());

            var queue=new SqsInputQueue(sqs,input,options);
            using var timeout=new CancellationTokenSource(TimeSpan.FromSeconds(60));
            var delivery=await queue.ReceiveAsync(timeout.Token);
            Assert.NotNull(delivery);
            await new DeliveryHandler(options).HandleAsync(queue,delivery!,ct=>processor.ProcessAsync(delivery!.Body,ct),timeout.Token);
            var received=new List<Message>();
            while(received.Count<2)
            {
                var response=await sqs.ReceiveMessageAsync(new ReceiveMessageRequest{QueueUrl=output,WaitTimeSeconds=1,MaxNumberOfMessages=10},timeout.Token);
                received.AddRange(response.Messages ?? []);
            }
            var work=received.Select(x=>JsonSerializer.Deserialize<ProductionWtgDayWorkV2>(x.Body)!).ToArray();
            Assert.Contains(work,x=>x.WorkKind==HarmonizationWorkKinds.ProductionWtgDays && x.WtgNames.SequenceEqual(new[]{"T1","T2"}));
            Assert.Contains(work,x=>x.WorkKind==HarmonizationWorkKinds.ProductionSnapshotReconciliation);
            // Wait beyond visibility: an empty immediate read alone would not prove deletion.
            await Task.Delay(TimeSpan.FromSeconds(4),timeout.Token);
            var remaining=await sqs.ReceiveMessageAsync(new ReceiveMessageRequest{QueueUrl=input,WaitTimeSeconds=1},timeout.Token);
            Assert.True(remaining.Messages is null || remaining.Messages.Count==0);
        }
        finally
        {
            await container.DeleteIfExistsAsync();
            await sqs.DeleteQueueAsync(input); await sqs.DeleteQueueAsync(output);
        }
    }
}
