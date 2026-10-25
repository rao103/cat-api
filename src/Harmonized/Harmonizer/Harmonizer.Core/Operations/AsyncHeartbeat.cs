using Harmonizer.Core.Interfaces;

namespace Harmonizer.Core.Operations;

/// <summary>Renews a bounded operation's ownership. Renewal failure cancels its work and is observed before success.</summary>
public sealed class AsyncHeartbeat : IAsyncDisposable
{
    private readonly CancellationTokenSource stop = new();
    private readonly Task loop;
    private Exception? failure;

    public AsyncHeartbeat(TimeSpan interval, Func<CancellationToken, Task> renew, CancellationTokenSource operation,
        ITelemetry<AsyncHeartbeat> telemetry, Func<IReadOnlyDictionary<string, object?>>? scopeState = null)
    {
        loop = RunAsync(interval, renew, operation, telemetry, scopeState);
    }
    private async Task RunAsync(TimeSpan interval, Func<CancellationToken, Task> renew, CancellationTokenSource operation,
        ITelemetry<AsyncHeartbeat> telemetry, Func<IReadOnlyDictionary<string, object?>>? scopeState)
    {
        using var timer = new PeriodicTimer(interval);
        try
        {
            while (await timer.WaitForNextTickAsync(stop.Token))
            {
                using var scope = scopeState is null ? null : telemetry.BeginScope(scopeState());
                await renew(stop.Token);
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested) { }
        catch (Exception exception)
        {
            using var scope = scopeState is null ? null : telemetry.BeginScope(scopeState());
            failure = exception;
            telemetry.Warning(exception, "Ownership heartbeat stopped after renewal failure. ErrorCode={ErrorCode}",
                exception.GetType().Name);
            await operation.CancelAsync();
        }
    }
    public async Task StopAsync()
    {
        await stop.CancelAsync();
        await loop;
        if (failure is not null)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(failure).Throw();
        }
    }
    public async ValueTask DisposeAsync()
    {
        await stop.CancelAsync();
        await loop;
        stop.Dispose();
    }
}
