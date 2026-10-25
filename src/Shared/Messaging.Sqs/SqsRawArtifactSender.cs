using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using CAT.Messaging.Contracts;

namespace CAT.Messaging.Sqs;

/// <summary>Common bounded SQS transport. Only explicit acknowledgments count as delivery.</summary>
public sealed class SqsRawArtifactSender : IDisposable
{
    // An application budget, deliberately below the service maximum; also bounds each batch body total.
    private const int MaxBatchBodyBytes = 240_000;
    private const int MaxAttempts = 3;
    private readonly IAmazonSQS _client;
    private readonly string _queueUrl;
    private readonly SemaphoreSlim _inFlight;

    public SqsRawArtifactSender(IAmazonSQS client, string queueUrl, int maxInFlightSends = 10)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentException.ThrowIfNullOrWhiteSpace(queueUrl);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxInFlightSends);
        _client = client;
        _queueUrl = queueUrl;
        _inFlight = new SemaphoreSlim(maxInFlightSends, maxInFlightSends);
    }

    public async Task PublishAsync(RawArtifactAvailableV2 message, CancellationToken cancellationToken)
    {
        if (!await PublishBatchAsync([message], cancellationToken))
            throw new InvalidOperationException("SQS publication could not be confirmed after retries.");
    }

    public async Task<bool> PublishBatchAsync(IReadOnlyList<RawArtifactAvailableV2> messages, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(messages);
        cancellationToken.ThrowIfCancellationRequested();
        // Validate the complete set before sending any entry. Serialize once so retries are identical.
        var entries = messages.Select(message =>
        {
            ArgumentNullException.ThrowIfNull(message);
            message.Validate();
            var body = JsonSerializer.Serialize(message);
            var size = Encoding.UTF8.GetByteCount(body);
            if (size > MaxBatchBodyBytes) throw new InvalidDataException("Raw envelope exceeds the application message budget.");
            return (Body: body, Size: size);
        }).ToArray();
        var trace = CreateTraceAttributes();
        var batch = new List<SendMessageBatchRequestEntry>(10);
        var batchBytes = 0;
        foreach (var entry in entries)
        {
            if (batch.Count == 10 || batchBytes + entry.Size > MaxBatchBodyBytes)
            {
                if (!await SendBatchAsync(batch, cancellationToken)) return false;
                batch.Clear();
                batchBytes = 0;
            }
            batch.Add(new SendMessageBatchRequestEntry
            {
                // Request-local ID only. Artifact identity lives in the immutable message body.
                Id = batch.Count.ToString(CultureInfo.InvariantCulture),
                MessageBody = entry.Body,
                MessageAttributes = trace,
            });
            batchBytes += entry.Size;
        }
        return batch.Count == 0 || await SendBatchAsync(batch, cancellationToken);
    }

    private async Task<bool> SendBatchAsync(List<SendMessageBatchRequestEntry> batch, CancellationToken cancellationToken)
    {
        var pending = batch.ToDictionary(entry => entry.Id, StringComparer.Ordinal);
        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await _inFlight.WaitAsync(cancellationToken);
            try
            {
                var response = await _client.SendMessageBatchAsync(new SendMessageBatchRequest
                {
                    QueueUrl = _queueUrl,
                    Entries = pending.Values.ToList(),
                }, cancellationToken);
                var failedIds = (response.Failed ?? []).Select(entry => entry.Id).ToHashSet(StringComparer.Ordinal);
                foreach (var success in response.Successful ?? [])
                    if (!failedIds.Contains(success.Id) && !string.IsNullOrWhiteSpace(success.MessageId))
                        pending.Remove(success.Id);
                if ((response.Failed ?? []).Any(entry => entry.SenderFault == true)) return false;
                if (pending.Count == 0) return true;
            }
            catch (OperationCanceledException) { throw; }
            catch (Exception) when (attempt < MaxAttempts)
            {
                // Ambiguous response: retry only the entries not previously confirmed.
            }
            finally
            {
                _inFlight.Release();
            }
            if (attempt < MaxAttempts)
                await Task.Delay(TimeSpan.FromMilliseconds(200 * attempt), cancellationToken);
        }
        return false;
    }

    private static Dictionary<string, MessageAttributeValue> CreateTraceAttributes()
    {
        var attributes = new Dictionary<string, MessageAttributeValue>();
        if (Activity.Current is not { IdFormat: ActivityIdFormat.W3C, Id: not null } activity) return attributes;
        attributes["traceparent"] = new() { DataType = "String", StringValue = activity.Id };
        if (!string.IsNullOrEmpty(activity.TraceStateString))
            attributes["tracestate"] = new() { DataType = "String", StringValue = activity.TraceStateString };
        return attributes;
    }

    public void Dispose() => _inFlight.Dispose();
}
