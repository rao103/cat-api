using ProductionIngestor.Interfaces;

namespace ProductionIngestor.Operations.Execution;

/// <summary>
/// Renews one run's lease and cancels processing when ownership can no longer be confirmed.
/// The caller owns the cancellation sources and awaits disposal before releasing the lease.
/// </summary>
internal sealed class LeaseRenewalGuard : IAsyncDisposable
{
    private readonly IExtractionCursorRepository _repository;
    private readonly IIngestionTelemetry<LeaseRenewalGuard> _telemetry;
    private readonly IngestionRunContext _context;
    private readonly string _ownerId;
    private readonly TimeSpan _leaseDuration;
    private readonly CancellationTokenSource _processing;
    private readonly CancellationTokenSource _renewalStop;
    private readonly Task _renewalTask;
    private int _leaseLost;

    public LeaseRenewalGuard(
        IExtractionCursorRepository repository,
        IIngestionTelemetry<LeaseRenewalGuard> telemetry,
        IngestionRunContext context,
        string ownerId,
        TimeSpan leaseDuration,
        CancellationTokenSource processing,
        CancellationTokenSource renewalStop)
    {
        _repository = repository;
        _telemetry = telemetry;
        _context = context;
        _ownerId = ownerId;
        _leaseDuration = leaseDuration;
        _processing = processing;
        _renewalStop = renewalStop;
        _renewalTask = RenewLoopAsync();
    }

    public bool LeaseLost => Volatile.Read(ref _leaseLost) != 0;

    private async Task RenewLoopAsync()
    {
        var renewalInterval = TimeSpan.FromTicks(_leaseDuration.Ticks / 3);
        var request = _context.Request;

        try
        {
            while (!_renewalStop.IsCancellationRequested)
            {
                using var attempt = CancellationTokenSource.CreateLinkedTokenSource(_renewalStop.Token);
                attempt.CancelAfter(renewalInterval);

                var renewed = await _repository.TryRenewLeaseAsync(
                    request.WindfarmId, request.SourceType, _ownerId,
                    _context.Lease.LeaseToken, _leaseDuration, attempt.Token);

                if (!renewed)
                {
                    _telemetry.IngestionInformation(request.WindfarmName, request.SourceType,
                        "Lease renewal was rejected; stopping ingestion without advancing the cursor.");
                    await StopProcessingAsync();
                    return;
                }

                await Task.Delay(renewalInterval, _renewalStop.Token);
            }
        }
        catch (OperationCanceledException) when (_renewalStop.IsCancellationRequested)
        {
            // Normal shutdown of the renewal loop, not loss of ownership.
        }
        catch (Exception error)
        {
            _telemetry.SourceContactFailed(request.WindfarmName, request.SourceType, error);
            await StopProcessingAsync();
        }
    }

    private async Task StopProcessingAsync()
    {
        Interlocked.Exchange(ref _leaseLost, 1);
        await _processing.CancelAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _renewalStop.CancelAsync();
        await _renewalTask;
    }
}