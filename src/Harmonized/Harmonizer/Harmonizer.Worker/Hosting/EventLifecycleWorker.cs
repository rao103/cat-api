using Harmonizer.Core.Configuration;
using Harmonizer.Core.Interfaces;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Harmonizer.Worker.Hosting;

/// <summary>Runs registered lifecycle sources even when the inbound queue is idle.</summary>
public sealed class EventLifecycleWorker(IEnumerable<IEventLifecycleHandler> lifecycleHandlers,
    IOptions<HarmonizerOptions> options, ITelemetry<EventLifecycleWorker> telemetry) : BackgroundService
{
    private readonly IReadOnlyList<IEventLifecycleHandler> handlers = lifecycleHandlers.ToArray();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!options.Value.EventLifecycle.Enabled)
        {
            telemetry.Information("Autonomous event lifecycle polling is disabled by configuration.");
            return;
        }
        telemetry.Information("Event lifecycle polling started. SourceCount={SourceCount} DaysPerPass={DaysPerPass}",
            handlers.Count, options.Value.EventLifecycle.DaysPerPass);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.WhenAll(handlers.Select(handler => RunSourceAsync(handler, stoppingToken)));
                await Task.Delay(TimeSpan.FromSeconds(options.Value.EventLifecycle.PollSeconds), stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
        }
    }

    private async Task RunSourceAsync(IEventLifecycleHandler handler, CancellationToken cancellationToken)
    {
        using var scope = telemetry.BeginScope(new Dictionary<string, object?> { ["SourceType"] = handler.SourceType });
        try { await handler.ProcessDueAsync(cancellationToken); }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { throw; }
        catch (Exception error)
        {
            telemetry.Warning(error, "Event lifecycle source polling failed. ErrorCode={ErrorCode}", error.GetType().Name);
            await Task.Delay(TimeSpan.FromSeconds(options.Value.EventLifecycle.RetrySeconds), cancellationToken);
        }
    }
}
