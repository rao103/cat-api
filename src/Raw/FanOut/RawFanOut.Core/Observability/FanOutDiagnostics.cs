using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace CAT.RawFanOut.Observability;

// Uses .NET instrumentation APIs only. Exporters and backend selection belong to the host.
public static class FanOutDiagnostics
{
    public const string Name = "CAT.RawFanOut";
    public static readonly ActivitySource Activities = new(Name);
    public static readonly Meter Meter = new(Name);
    public static readonly Counter<long> Deliveries = Meter.CreateCounter<long>("rawfanout.deliveries");
    public static readonly Histogram<double> DeliveryDuration = Meter.CreateHistogram<double>("rawfanout.delivery.duration", "s");
    public static readonly Histogram<double> StageDuration = Meter.CreateHistogram<double>("rawfanout.stage.duration", "s");
    public static readonly Counter<long> PublishRetries = Meter.CreateCounter<long>("rawfanout.publish.retries");

    public static async Task<T> MeasureAsync<T>(string stage, Func<Task<T>> operation)
    {
        using var activity = Activities.StartActivity(stage);
        var started = Stopwatch.GetTimestamp();
        var outcome = "success";
        try
        {
            return await operation();
        }
        catch (OperationCanceledException)
        {
            outcome = "cancelled";
            activity?.SetTag("operation.cancelled", true);
            throw;
        }
        catch (Exception error)
        {
            outcome = "failure";
            activity?.SetStatus(ActivityStatusCode.Error);
            activity?.SetTag("error.type", error.GetType().FullName);
            throw;
        }
        finally
        {
            StageDuration.Record(Stopwatch.GetElapsedTime(started).TotalSeconds,
                new KeyValuePair<string, object?>("stage", stage),
                new KeyValuePair<string, object?>("outcome", outcome));
        }
    }

    public static Task MeasureAsync(string stage, Func<Task> operation) =>
        MeasureAsync(stage, async () => { await operation(); return true; });
}
