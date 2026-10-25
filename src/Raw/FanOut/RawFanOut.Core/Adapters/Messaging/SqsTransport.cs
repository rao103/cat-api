using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Text;
using Amazon.SQS;
using Amazon.SQS.Model;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using CAT.RawFanOut.Observability;

namespace CAT.RawFanOut.Adapters.Messaging;

public sealed class SqsBatchTransport(IAmazonSQS sqs, string queueUrl) : IBatchTransport
{
    public async Task<PublishResult> SendAsync(IReadOnlyList<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        using var activity = FanOutDiagnostics.Activities.StartActivity("sqs.send", ActivityKind.Producer);
        activity?.SetTag("messaging.system", "aws_sqs");
        activity?.SetTag("messaging.batch.message_count", messages.Count);
        var entries = messages.Select(message => new SendMessageBatchRequestEntry
        {
            Id = message.Id,
            MessageBody = message.Body,
            MessageAttributes = CreateTraceAttributes()
        }).ToList();

        // SQS counts attribute data as well as body bytes. Default configured budgets
        // leave ample room, but enforce the physical limit even for custom budgets.
        var totalBytes = entries.Sum(entry => Encoding.UTF8.GetByteCount(entry.MessageBody) +
            entry.MessageAttributes.Sum(attribute => Encoding.UTF8.GetByteCount(attribute.Key) +
                Encoding.UTF8.GetByteCount(attribute.Value.DataType) + Encoding.UTF8.GetByteCount(attribute.Value.StringValue)));
        if (totalBytes > 1_048_576)
            throw new InvalidDataException("SQS request exceeds its byte limit including trace attributes.");

        try
        {
            var response = await sqs.SendMessageBatchAsync(new SendMessageBatchRequest
            {
                QueueUrl = queueUrl,
                Entries = entries
            }, cancellationToken);

            if (response.Failed is { Count: > 0 })
                activity?.SetStatus(ActivityStatusCode.Error);
            return new PublishResult(
                (response.Successful ?? []).Select(result => result.Id).ToArray(),
                (response.Failed ?? []).Select(result => new PublishFailure(result.Id, result.SenderFault == true, result.Code)).ToArray());
        }
        catch (Exception error)
        {
            activity?.SetStatus(ActivityStatusCode.Error);
            activity?.SetTag("error.type", error.GetType().FullName);
            throw;
        }
    }

    private static Dictionary<string, MessageAttributeValue> CreateTraceAttributes()
    {
        var attributes = new Dictionary<string, MessageAttributeValue>(StringComparer.Ordinal);
        var current = Activity.Current;
        if (current?.IdFormat != ActivityIdFormat.W3C || current.Id is null)
            return attributes;

        attributes["traceparent"] = new MessageAttributeValue { DataType = "String", StringValue = current.Id };
        if (!string.IsNullOrEmpty(current.TraceStateString))
            attributes["tracestate"] = new MessageAttributeValue { DataType = "String", StringValue = current.TraceStateString };
        return attributes;
    }
}

public sealed class SqsInputQueue(IAmazonSQS sqs, string queueUrl, FanOutOptions options) : IInputQueue
{
    public async Task<QueueDelivery?> ReceiveAsync(CancellationToken cancellationToken)
    {
        var response = await sqs.ReceiveMessageAsync(new ReceiveMessageRequest
        {
            QueueUrl = queueUrl,
            MaxNumberOfMessages = 1,
            WaitTimeSeconds = 20,
            VisibilityTimeout = options.VisibilitySeconds,
            MessageSystemAttributeNames = ["All"],
            MessageAttributeNames = ["traceparent", "tracestate"]
        }, cancellationToken);

        var message = response.Messages?.SingleOrDefault();
        if (message is null)
            return null;

        return new QueueDelivery(message.MessageId, message.ReceiptHandle, message.Body,
            message.Attributes ?? new Dictionary<string, string>())
        {
            TraceParent = ReadAttribute(message, "traceparent"),
            TraceState = ReadAttribute(message, "tracestate")
        };
    }

    public async Task RenewAsync(QueueDelivery delivery, CancellationToken cancellationToken)
    {
        await sqs.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
        {
            QueueUrl = queueUrl,
            ReceiptHandle = delivery.ReceiptHandle,
            VisibilityTimeout = options.VisibilitySeconds
        }, cancellationToken);
    }

    public async Task DeleteAsync(QueueDelivery delivery, CancellationToken cancellationToken)
    {
        await sqs.DeleteMessageAsync(new DeleteMessageRequest
        {
            QueueUrl = queueUrl,
            ReceiptHandle = delivery.ReceiptHandle
        }, cancellationToken);
    }

    private static string? ReadAttribute(Message message, string name) =>
        message.MessageAttributes is not null && message.MessageAttributes.TryGetValue(name, out var value)
            && value.DataType == "String" ? value.StringValue : null;
}
