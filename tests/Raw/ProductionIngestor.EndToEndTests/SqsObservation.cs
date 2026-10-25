using System.Text.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using ProductionIngestor.Contracts;

namespace ProductionIngestor.EndToEndTests;

/// <summary>
/// Bounded, non-destructive SQS observation. Never deletes or purges; restores visibility for
/// everything it receives. Cannot filter server-side by run id, so it inspects every message
/// received within the bounded window and only reports on the ones matching the expected run.
/// </summary>
public sealed class SqsObservation
{
    private readonly IAmazonSQS _client;
    private readonly string _queueUrl;

    public SqsObservation(IAmazonSQS client, string queueUrl)
    {
        _client = client;
        _queueUrl = queueUrl;
    }

    public async Task<RawArtifactAvailableV1?> ObserveAsync(
        string expectedRunId, TimeSpan observationWindow, CancellationToken cancellationToken)
    {
        RawArtifactAvailableV1? matched = null;
        var conflicting = new List<RawArtifactAvailableV1>();
        var deadline = DateTimeOffset.UtcNow + observationWindow;

        while (DateTimeOffset.UtcNow < deadline && !cancellationToken.IsCancellationRequested)
        {
            var remaining = deadline - DateTimeOffset.UtcNow;
            if (remaining <= TimeSpan.Zero)
                break;

            var response = await _client.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = _queueUrl,
                MaxNumberOfMessages = 10,
                WaitTimeSeconds = (int)Math.Min(20, Math.Max(1, remaining.TotalSeconds)),
                VisibilityTimeout = 5,
                MessageAttributeNames = ["All"],
            }, cancellationToken);

            if (response.Messages.Count == 0)
                continue;

            foreach (var message in response.Messages)
            {
                // Restore visibility immediately: this is observation only, never consumption.
                try
                {
                    await _client.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
                    {
                        QueueUrl = _queueUrl,
                        ReceiptHandle = message.ReceiptHandle,
                        VisibilityTimeout = 0,
                    }, cancellationToken);
                }
                catch (AmazonSQSException)
                {
                    // Message may have already become visible again; not fatal to observation.
                }

                RawArtifactAvailableV1? candidate;
                try
                {
                    candidate = JsonSerializer.Deserialize<RawArtifactAvailableV1>(message.Body);
                }
                catch (JsonException)
                {
                    continue;
                }

                if (candidate is null || candidate.RunId != expectedRunId)
                    continue;

                if (matched is null)
                {
                    matched = candidate;
                }
                else if (!AreEquivalent(matched, candidate))
                {
                    conflicting.Add(candidate);
                }
            }
        }

        if (conflicting.Count > 0)
            throw new InvalidOperationException(
                $"Observed {conflicting.Count} conflicting RawArtifactAvailableV1 message(s) for run '{expectedRunId}'.");

        return matched;
    }

    // Duplicate deliveries are permitted; only content differences for the same run id are a conflict.
    private static bool AreEquivalent(RawArtifactAvailableV1 left, RawArtifactAvailableV1 right)
        => left.SourceType == right.SourceType
            && left.WindfarmId == right.WindfarmId
            && left.DataDate == right.DataDate
            && left.ExtractionToken == right.ExtractionToken
            && left.BlobPath == right.BlobPath
            && (left.ShardPaths ?? []).SequenceEqual(right.ShardPaths ?? []);
}
