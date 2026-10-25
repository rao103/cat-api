using System.Diagnostics;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.Observability;
using Microsoft.Extensions.Hosting;

namespace Harmonizer.Worker.Hosting;

public sealed class NotificationWorker(
    INotificationOutbox outbox,
    INotificationPublisher publisher,
    ITelemetry<NotificationWorker> telemetry) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                foreach (var message in await outbox.ReadPendingAsync(100, stoppingToken))
                {
                    await PublishAsync(message, stoppingToken);
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception error)
            {
                telemetry.Warning(error, "Outbox publish deferred. ErrorCode={ErrorCode}", error.GetType().Name);
            }

            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);
        }
    }

    private async Task PublishAsync(Notification message, CancellationToken cancellationToken)
    {
        using var scope = telemetry.BeginScope(new Dictionary<string, object?>
        {
            ["OutboxId"] = message.Id,
            ["OperationId"] = message.OperationId,
            ["ExecutionId"] = message.ExecutionId,
            ["PipelineStage"] = "harmonizer.notification"
        });
        using var activity = HarmonizerDiagnostics.Activities.StartActivity("harmonizer.notification.publish", ActivityKind.Producer);
        activity?.SetTag("cat.operation.id", message.OperationId);
        activity?.SetTag("cat.execution.id", message.ExecutionId);
        activity?.SetTag("cat.pipeline.stage", "harmonizer.notification");
        try
        {
            // At-least-once delivery: replay after a send/commit crash uses the stable NotificationId.
            await publisher.PublishAsync(message, cancellationToken);
            await outbox.MarkSentAsync(message.Id, cancellationToken);
            activity?.SetStatus(ActivityStatusCode.Ok);
            telemetry.Information("Outbox notification published and marked sent.");
        }
        catch (Exception error) when (error is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            activity?.SetStatus(ActivityStatusCode.Error, error.GetType().Name);
            telemetry.Warning(error, "Outbox notification remains pending. ErrorCode={ErrorCode}", error.GetType().Name);
            throw;
        }
    }
}
