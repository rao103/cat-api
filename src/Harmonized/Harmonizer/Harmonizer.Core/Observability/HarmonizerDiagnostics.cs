using System.Diagnostics;
using System.Diagnostics.Metrics;
namespace Harmonizer.Core.Observability;

public static class HarmonizerDiagnostics
{
    public const string Name = "CAT.Harmonizer";
    public static ActivitySource Activities { get; } = new(Name);
    public static Meter Meter { get; } = new(Name);
    public static Counter<long> Committed { get; } = Meter.CreateCounter<long>("harmonizer.component.committed");
    public static Counter<long> Failed { get; } = Meter.CreateCounter<long>("harmonizer.work.failed");
    public static Counter<long> FenceRejected { get; } = Meter.CreateCounter<long>("harmonizer.fence.rejected");
    public static Counter<long> LifecycleDays { get; } = Meter.CreateCounter<long>("harmonizer.lifecycle.days");
    public static Counter<long> LifecycleFailures { get; } = Meter.CreateCounter<long>("harmonizer.lifecycle.failures");
    public static Histogram<double> LifecycleDuration { get; } = Meter.CreateHistogram<double>("harmonizer.lifecycle.duration", "s");
    public static Histogram<double> BatchDuration { get; } = Meter.CreateHistogram<double>("harmonizer.batch.duration", "s");
}
