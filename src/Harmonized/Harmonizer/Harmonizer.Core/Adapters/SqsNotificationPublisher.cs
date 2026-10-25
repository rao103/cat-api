using Amazon.SQS;
using Amazon.SQS.Model;
using Harmonizer.Core.Configuration;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Microsoft.Extensions.Options;
namespace Harmonizer.Core.Adapters;

public sealed class SqsNotificationPublisher(IAmazonSQS client, IOptions<HarmonizerOptions> options,
    ITelemetry<SqsNotificationPublisher> telemetry) : INotificationPublisher
{
    public async Task PublishAsync(Notification notification, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(options.Value.NotificationQueueUrl))
        {
            throw new InvalidOperationException("Notification queue is not configured; durable intents must remain pending.");
        }

        await client.SendMessageAsync(new SendMessageRequest
        {
            QueueUrl = options.Value.NotificationQueueUrl,
            MessageBody = notification.Body
        }, cancellationToken);
        telemetry.Debug("Outbox notification sent to SQS. OutboxId={OutboxId}", notification.Id);
    }
}
