using Amazon.SQS;
using Amazon.SQS.Model;
using Harmonizer.Core.Configuration;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Microsoft.Extensions.Options;
namespace Harmonizer.Core.Adapters;

public sealed class SqsWorkQueue(IAmazonSQS client, IOptions<HarmonizerOptions> options,
    ITelemetry<SqsWorkQueue> telemetry) : IWorkQueue
{
    public async Task<IReadOnlyList<QueueDelivery>> ReceiveAsync(int maximumMessages, CancellationToken cancellationToken)
    {
        var response = await client.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = options.Value.QueueUrl,
            MaxNumberOfMessages = maximumMessages,
            WaitTimeSeconds = options.Value.PollSeconds,
            VisibilityTimeout = options.Value.VisibilitySeconds,
            MessageSystemAttributeNames = ["ApproximateReceiveCount"]
        }, cancellationToken);
        var deliveries = (response.Messages ?? []).Select(message => new QueueDelivery(message.MessageId, message.ReceiptHandle, message.Body,
            message.Attributes is not null && message.Attributes.TryGetValue("ApproximateReceiveCount", out var count) &&
                int.TryParse(count, out var parsed) ? parsed : 1)).ToArray();
        if (deliveries.Length > 0)
        {
            telemetry.Debug("SQS deliveries received. DeliveryCount={DeliveryCount}", deliveries.Length);
        }

        return deliveries;
    }
    public async Task RenewVisibilityAsync(QueueDelivery delivery, CancellationToken cancellationToken)
    {
        try
        {
            await client.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
            {
                QueueUrl = options.Value.QueueUrl,
                ReceiptHandle = delivery.ReceiptHandle,
                VisibilityTimeout = options.Value.VisibilitySeconds
            }, cancellationToken);
        }
        catch (Exception error) when (error is not OperationCanceledException)
        {
            telemetry.Warning(error, "SQS visibility renewal failed. MessageId={MessageId}", delivery.MessageId);
            throw;
        }
    }

    public async Task DeleteAsync(QueueDelivery delivery, CancellationToken cancellationToken)
    {
        await client.DeleteMessageAsync(options.Value.QueueUrl, delivery.ReceiptHandle, cancellationToken);
        telemetry.Debug("SQS delivery deleted. MessageId={MessageId}", delivery.MessageId);
    }
}
