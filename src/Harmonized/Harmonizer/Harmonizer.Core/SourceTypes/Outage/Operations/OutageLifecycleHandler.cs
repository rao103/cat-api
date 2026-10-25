using Harmonizer.Core.Configuration;
using Harmonizer.Core.Interfaces;
using Harmonizer.Core.Models;
using Harmonizer.Core.SourceTypes.Outage.Interfaces;
using Microsoft.Extensions.Options;

namespace Harmonizer.Core.SourceTypes.Outage.Operations;

public sealed class OutageLifecycleHandler(IOutageControlRepository state, OutageLifecycleProcessor processor,
    IOptions<HarmonizerOptions> options, ITelemetry<OutageLifecycleHandler> telemetry) : IEventLifecycleHandler
{
    public string SourceType => "Outage";

    public async Task ProcessDueAsync(CancellationToken cancellationToken)
    {
        var farms = await state.GetDueFarmsAsync(options.Value.EventLifecycle.FarmsPerPoll, cancellationToken);
        var parallel = new ParallelOptions
        { MaxDegreeOfParallelism = options.Value.EventLifecycle.ConcurrentFarms, CancellationToken = cancellationToken };
        await Parallel.ForEachAsync(farms, parallel, async (farm, token) =>
        {
            try { await processor.ProcessFarmAsync(farm, token); }
            catch (LeaseUnavailableException)
            { telemetry.Debug("Another replica owns the Outage farm/source lease. WindfarmId={WindfarmId}", farm.WindfarmId); }
            catch (OperationCanceledException) when (token.IsCancellationRequested) { throw; }
            catch (Exception error)
            { telemetry.Warning(error, "Outage lifecycle farm retained for retry. WindfarmId={WindfarmId}", farm.WindfarmId); }
        });
    }
}
