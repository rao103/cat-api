using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
using CAT.RawFanOut.Observability;

namespace CAT.RawFanOut.Operations.Delivery;

public sealed class ReliableBatchPublisher : IWorkPublisher
{
    private const int MaximumEntriesPerRequest = 10;
    private readonly IBatchTransport transport;
    private readonly FanOutOptions options;
    private readonly ITelemetry<ReliableBatchPublisher> telemetry;

    public ReliableBatchPublisher(
        IBatchTransport transport,
        FanOutOptions options,
        ITelemetry<ReliableBatchPublisher> telemetry)
    {
        this.transport = transport;
        this.options = options;
        this.telemetry = telemetry;
    }

    public async Task PublishAsync(IReadOnlyList<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        var request = new List<OutgoingMessage>(MaximumEntriesPerRequest);
        var requestBytes = 0;

        foreach (var message in messages)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var messageBytes = Encoding.UTF8.GetByteCount(message.Body);
            if (messageBytes > options.MaxMessageBytes)
                throw new InvalidDataException("Message exceeds the configured byte limit.");

            if (request.Count == MaximumEntriesPerRequest || requestBytes + messageBytes > options.MaxBatchBytes)
            {
                await SendWithRetryAsync(request, cancellationToken);
                request.Clear();
                requestBytes = 0;
            }

            request.Add(message);
            requestBytes += messageBytes;
        }

        if (request.Count > 0)
            await SendWithRetryAsync(request, cancellationToken);
    }

    private async Task SendWithRetryAsync(List<OutgoingMessage> messages, CancellationToken cancellationToken)
    {
        var pending = messages.ToList();
        for (var attempt = 1; attempt <= options.PublishAttempts; attempt++)
        {
            var response = await transport.SendAsync(pending, cancellationToken);
            ValidateResponse(pending, response);

            if (response.Failed.Any(failure => failure.Permanent))
                throw new InvalidDataException("Queue rejected one or more messages permanently.");

            var failedIds = response.Failed.Select(failure => failure.Id).ToHashSet(StringComparer.Ordinal);
            pending = pending.Where(message => failedIds.Contains(message.Id)).ToList();
            if (pending.Count == 0)
                return;

            if (attempt < options.PublishAttempts)
            {
                telemetry.Warning($"Retrying {pending.Count} failed queue entries after send attempt {attempt}");
                FanOutDiagnostics.PublishRetries.Add(1);
                var delay = TimeSpan.FromMilliseconds(100 * Math.Pow(2, attempt - 1) + Random.Shared.Next(100));
                await Task.Delay(delay, cancellationToken);
            }
        }

        throw new IOException("Queue batch retries exhausted. Parent remains unacknowledged.");
    }

    private static void ValidateResponse(IReadOnlyList<OutgoingMessage> sent, PublishResult response)
    {
        var expectedIds = sent.Select(message => message.Id).ToHashSet(StringComparer.Ordinal);
        var returnedIds = response.Succeeded.Concat(response.Failed.Select(failure => failure.Id)).ToArray();
        var uniqueReturnedIds = returnedIds.ToHashSet(StringComparer.Ordinal);

        if (returnedIds.Length != expectedIds.Count || uniqueReturnedIds.Count != returnedIds.Length ||
            !expectedIds.SetEquals(uniqueReturnedIds))
        {
            throw new IOException("Incomplete or invalid queue batch response.");
        }
    }
}
