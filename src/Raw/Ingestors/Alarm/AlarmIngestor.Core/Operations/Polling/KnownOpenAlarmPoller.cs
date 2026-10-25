using System.Runtime.CompilerServices;
using AlarmIngestor.Configuration;
using AlarmIngestor.Interfaces;
using AlarmIngestor.Models;

namespace AlarmIngestor.Operations.Polling;

/// <summary>
/// Polls known-open alarm IDs in bounded per-turbine batches without any start-date
/// restriction (Section "Normal Run" step 7). Processes the complete requested-id set; IDs the
/// source does not return remain unresolved and tracked rather than assumed closed/deleted
/// (Section "Missing polled IDs"). Yields one batch's response at a time so a caller can write,
/// publish and checkpoint each batch before requesting the next, bounding memory to one batch.
/// </summary>
public sealed class KnownOpenAlarmPoller
{
    private readonly IAlarmSourceAdapter _adapter;
    private readonly int _batchSize;

    public KnownOpenAlarmPoller(IAlarmSourceAdapter adapter, int batchSize)
    {
        _adapter = adapter;
        _batchSize = batchSize;
    }

    public async IAsyncEnumerable<IReadOnlyList<AlarmEventModel>> PollBatchesAsync(
        string windfarmId, TurbineIdentity turbine, IReadOnlyList<long> knownOpenAlarmIds, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        for (var i = 0; i < knownOpenAlarmIds.Count; i += _batchSize)
        {
            var batch = knownOpenAlarmIds.Skip(i).Take(_batchSize).ToList();
            var response = await _adapter.PollKnownAlarmsAsync(windfarmId, turbine.TurbineRdspp, turbine.ItemId, batch, cancellationToken);
            yield return response;
        }
    }
}
