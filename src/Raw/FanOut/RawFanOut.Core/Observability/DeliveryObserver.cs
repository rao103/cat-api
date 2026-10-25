using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Text.Json;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;

namespace CAT.RawFanOut.Observability;

// Observes the entire delivery, including visibility renewal and receipt deletion.
public sealed class DeliveryObserver
{
    private readonly FanOutOptions options;
    private readonly ITelemetry<DeliveryObserver> telemetry;

    public DeliveryObserver(FanOutOptions options, ITelemetry<DeliveryObserver> telemetry)
    {
        this.options = options;
        this.telemetry = telemetry;
    }

    public async Task ObserveAsync(
        QueueDelivery delivery,
        bool deadLetter,
        Func<CancellationToken, Task> operation,
        CancellationToken stoppingToken)
    {
        var queueRole = deadLetter ? "raw-dlq" : "raw";
        var parent = TraceContext.Extract(delivery.TraceParent, delivery.TraceState);
        using var activity = FanOutDiagnostics.Activities.StartActivity("sqs.process", ActivityKind.Consumer, parent);
        activity?.SetTag("messaging.system", "aws_sqs");
        activity?.SetTag("messaging.destination.name", queueRole);
        activity?.SetTag("messaging.message.id", delivery.MessageId);

        var envelope = TryReadEnvelope(delivery.Body);
        var source = envelope?.SourceType ?? "Unknown";
        var runId = envelope?.RunId;
        using var scope = telemetry.BeginScope(new Dictionary<string, object?>
        {
            ["MessageId"] = delivery.MessageId,
            ["QueueRole"] = queueRole,
            ["ArtifactId"] = envelope?.ArtifactId,
            ["RunId"] = envelope?.RunId,
            ["OperationId"] = envelope?.OperationId,
            ["AttemptId"] = envelope?.AttemptId,
            ["SourceType"] = source,
            ["WindfarmId"] = envelope?.WindfarmId,
            ["ExtractionToken"] = envelope?.ExtractionToken,
            ["DataDate"] = envelope?.DataDate?.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture),
            ["CorrelationId"] = envelope?.TraceId,
            ["TraceId"] = activity?.TraceId.ToString(),
            ["SpanId"] = activity?.SpanId.ToString()
        });

        var started = Stopwatch.GetTimestamp();
        var outcome = "success";
        telemetry.Information($"Processing queue delivery for {queueRole}.", runId);
        try
        {
            await operation(stoppingToken);
            if (deadLetter)
                telemetry.Warning("Dead-letter archive committed and queue receipt deleted.", runId);
            else
                telemetry.Information("Raw delivery processed and queue receipt deleted.", runId);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            outcome = "cancelled";
            activity?.SetTag("operation.cancelled", true);
            telemetry.Information("Shutdown interrupted delivery; redelivery may be required.", runId);
            throw;
        }
        catch (Exception error)
        {
            outcome = "failure";
            activity?.SetStatus(ActivityStatusCode.Error);
            activity?.SetTag("error.type", error.GetType().FullName);
            // SDK exception messages may contain object URLs, SQL or credentials.
            // Keep diagnostic type and component-only stack frames, never raw exception text.
            telemetry.Error(
                $"Delivery failed; queue may redeliver. ErrorType={error.GetType().FullName} ErrorCode={SafeErrorCode(error)} FailureLocation={SafeFailureLocation(error)}",
                error, runId);
            throw;
        }
        finally
        {
            var tags = new TagList { { "queue.role", queueRole }, { "source.type", source }, { "outcome", outcome } };
            FanOutDiagnostics.Deliveries.Add(1, tags);
            FanOutDiagnostics.DeliveryDuration.Record(Stopwatch.GetElapsedTime(started).TotalSeconds, tags);
        }
    }

    private RawEnvelope? TryReadEnvelope(string body)
    {
        try { return RawEnvelope.Parse(body, options); }
        catch (Exception error) when (error is JsonException or InvalidDataException or InvalidOperationException or FormatException)
        {
            return null; // Business processing/DLQ persistence decides the actual outcome.
        }
    }

    private static string SafeErrorCode(Exception error) => error switch
    {
        Npgsql.PostgresException databaseError => "postgres:" + databaseError.SqlState,
        Azure.RequestFailedException storageError => "azure-http:" + storageError.Status,
        Amazon.SQS.AmazonSQSException queueError => "sqs-http:" + (int)queueError.StatusCode,
        InvalidDataException => "invalid-artifact-or-message",
        OperationCanceledException => "processing-cancelled-or-deadline",
        IOException => "io-or-publication-failure",
        _ => "unclassified"
    };

    private static string SafeFailureLocation(Exception error)
    {
        var frames = new StackTrace(error, false).GetFrames() ?? [];
        var names = frames.Select(frame => frame.GetMethod())
            .Where(method => method?.DeclaringType?.Namespace?.StartsWith("CAT.RawFanOut", StringComparison.Ordinal) == true)
            .Take(6)
            .Select(method => $"{method!.DeclaringType!.FullName}.{method.Name}");
        return string.Join(";", names);
    }
}
