using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CAT.RawFanOut.Configuration;
using CAT.RawFanOut.Interfaces;
using CAT.RawFanOut.Models;
namespace CAT.RawFanOut.Operations.Delivery;

public sealed class DeliveryHandler(FanOutOptions options, ITelemetry<DeliveryHandler>? telemetry = null)
{
    public async Task HandleAsync(IInputQueue queue, QueueDelivery delivery,
        Func<CancellationToken, Task> process, CancellationToken shutdown)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(shutdown);
        deadline.CancelAfter(TimeSpan.FromSeconds(options.MaxProcessingSeconds));
        using var heartbeatStop = new CancellationTokenSource();
        // Renewal failure cancels provider work and prevents acknowledgement.
        var heartbeat = RenewAsync();
        Exception? processingFailure = null;
        try
        {
            await process(deadline.Token);
            deadline.Token.ThrowIfCancellationRequested();
            heartbeatStop.Cancel();
            await heartbeat;
            deadline.Token.ThrowIfCancellationRequested();
            await queue.DeleteAsync(delivery, deadline.Token);
        }
        catch (Exception ex) { processingFailure = ex; throw; }
        finally
        {
            heartbeatStop.Cancel();
            try { await heartbeat; }
            catch when (processingFailure is not null) { /* Preserve the original failure. */ }
        }
        async Task RenewAsync()
        {
            using var stop = CancellationTokenSource.CreateLinkedTokenSource(heartbeatStop.Token, deadline.Token);
            try
            {
                while (true)
                {
                    await Task.Delay(TimeSpan.FromSeconds(options.HeartbeatSeconds), stop.Token);
                    await queue.RenewAsync(delivery, stop.Token);
                }
            }
            catch (OperationCanceledException) when (heartbeatStop.IsCancellationRequested || deadline.IsCancellationRequested) { }
            catch (Exception error)
            {
                telemetry?.Warning($"Visibility renewal failed; cancelling processing. ErrorType={error.GetType().FullName}");
                deadline.Cancel();
                throw;
            }
        }
    }
}
