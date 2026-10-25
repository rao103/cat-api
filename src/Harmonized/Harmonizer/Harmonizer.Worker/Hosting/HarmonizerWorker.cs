using System.Diagnostics;
using Harmonizer.Core.Configuration;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.Observability;
using Harmonizer.Core.Operations;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Harmonizer.Worker.Hosting;

public sealed class HarmonizerWorker(
    IWorkQueue queue,
    HarmonizationDispatcher dispatcher,
    IOptions<HarmonizerOptions> options,
    ITelemetry<HarmonizerWorker> telemetry,
    ITelemetry<AsyncHeartbeat> heartbeatTelemetry) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var deliveries = await queue.ReceiveAsync(options.Value.ConcurrentMessages, stoppingToken);
                // Receive only available processing slots; visibility renewal starts immediately for each delivery.
                await Task.WhenAll(deliveries.Select(delivery => ProcessDeliveryAsync(delivery, stoppingToken)));
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception error)
            {
                telemetry.Error(error, "SQS receive failed. ErrorCode={ErrorCode}", error.GetType().Name);
                await Task.Delay(TimeSpan.FromSeconds(5 + Random.Shared.Next(5)), stoppingToken);
            }
        }
    }

    private async Task ProcessDeliveryAsync(QueueDelivery delivery, CancellationToken stoppingToken)
    {
        using var scope = telemetry.BeginScope(new Dictionary<string, object?>
        {
            ["MessageId"] = delivery.MessageId,
            ["ReceiveCount"] = delivery.ReceiveCount
        });
        telemetry.Debug("Harmonizer delivery received. ReceiveCount={ReceiveCount}", delivery.ReceiveCount);
        using var activity = HarmonizerDiagnostics.Activities.StartActivity("harmonizer.delivery", ActivityKind.Consumer);
        activity?.SetTag("messaging.system", "aws_sqs");
        activity?.SetTag("messaging.message.id", delivery.MessageId);
        using var operation = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var correlation = OperationIdentity.Untracked;
        await using var heartbeat = new AsyncHeartbeat(
            TimeSpan.FromSeconds(options.Value.HeartbeatSeconds),
            token => queue.RenewVisibilityAsync(delivery, token), operation, heartbeatTelemetry,
            () =>
            {
                // The renewal loop starts before catalog lookup; read validated metadata when available.
                var identity = Volatile.Read(ref correlation);
                return new Dictionary<string, object?>
                {
                    ["OperationId"] = identity.OperationId,
                    ["ExecutionId"] = identity.ExecutionId,
                    ["PipelineStage"] = "harmonizer"
                };
            });
        IDisposable? operationScope = null;
        try
        {
            var work = await dispatcher.ProcessAsync(delivery.Body, identity =>
            {
                Volatile.Write(ref correlation, identity);
                operationScope = telemetry.BeginScope(new Dictionary<string, object?>
                {
                    ["OperationId"] = identity.OperationId,
                    ["ExecutionId"] = identity.ExecutionId,
                    ["PipelineStage"] = "harmonizer"
                });
                activity?.SetTag("cat.operation.id", identity.OperationId);
                activity?.SetTag("cat.execution.id", identity.ExecutionId);
                activity?.SetTag("cat.pipeline.stage", "harmonizer");
            }, operation.Token);
            await heartbeat.StopAsync();
            operation.Token.ThrowIfCancellationRequested();
            await queue.DeleteAsync(delivery, stoppingToken);
            activity?.SetStatus(ActivityStatusCode.Ok);
            telemetry.Information("Work durably accepted and delivery deleted. BatchId={BatchId} SourceType={SourceType}",
                work.Batch.BatchId, work.SourceType);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host shutdown leaves unfinished work available for redelivery.
        }
        catch (LeaseUnavailableException)
        {
            telemetry.Debug("Delivery retained while another owner or durable dependency is pending.");
        }
        catch (Exception error)
        {
            activity?.SetStatus(ActivityStatusCode.Error, error.GetType().Name);
            HarmonizerDiagnostics.Failed.Add(1, new KeyValuePair<string, object?>("error.type", error.GetType().Name));
            telemetry.Warning(error, "Work retained for retry/DLQ. ErrorCode={ErrorCode}", error.GetType().Name);
        }
        finally
        {
            operationScope?.Dispose();
        }
    }
}
