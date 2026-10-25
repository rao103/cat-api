using AlarmIngestor.Configuration;
using AlarmIngestor.Interfaces;
using AlarmIngestor.Models;
using AlarmIngestor.Operations.Polling;

namespace AlarmIngestor.UnitTests;

/// <summary>
/// <see cref="KnownOpenAlarmPoller"/>: no start-date restriction is ever applied, requests are
/// chunked into bounded per-turbine batches, and the full requested-id set is queried across
/// batches rather than assuming one page covers it.
/// </summary>
public sealed class KnownOpenAlarmPollerTests
{
    private static readonly TurbineIdentity Turbine = new("RDSPP-1", "ITEM-1");

    private static AlarmEventModel MakeEvent(long alarmId) => new(
        "farm", "RDSPP-1", "ITEM-1", alarmId, "100", "text", DateTime.UtcNow, null, "Alarm", "Good", false, DateTime.UtcNow, 0);

    [Fact]
    public async Task ChunksRequestedIdsIntoBoundedBatches()
    {
        var adapter = new FakeAdapter();
        var poller = new KnownOpenAlarmPoller(adapter, batchSize: 2);

        var result = new List<AlarmEventModel>();
        await foreach (var batch in poller.PollBatchesAsync("farm", Turbine, [1, 2, 3, 4, 5], CancellationToken.None))
        {
            result.AddRange(batch);
        }

        Assert.Equal(3, adapter.BatchesRequested.Count);
        Assert.Equal([1L, 2L], adapter.BatchesRequested[0]);
        Assert.Equal([3L, 4L], adapter.BatchesRequested[1]);
        Assert.Equal([5L], adapter.BatchesRequested[2]);
        Assert.Equal(5, result.Count);
    }

    [Fact]
    public async Task EmptyKnownOpenSetSkipsTheSourceEntirely()
    {
        var adapter = new FakeAdapter();
        var poller = new KnownOpenAlarmPoller(adapter, batchSize: 500);

        var result = new List<AlarmEventModel>();
        await foreach (var batch in poller.PollBatchesAsync("farm", Turbine, [], CancellationToken.None))
        {
            result.AddRange(batch);
        }

        Assert.Empty(result);
        Assert.Empty(adapter.BatchesRequested);
    }

    private sealed class FakeAdapter : IAlarmSourceAdapter
    {
        public List<IReadOnlyList<long>> BatchesRequested { get; } = [];

        public Task<long?> GetMaxAlarmIdAsync(string turbineRdspp, DateTime minRetrievalDateUtc, IReadOnlyList<string> alarmCodes, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<IReadOnlyList<AlarmEventModel>> DiscoverPageAsync(
            string windfarmId, string turbineRdspp, string itemId, long afterAlarmId, long upperBoundAlarmId,
            DateTime minRetrievalDateUtc, IReadOnlyList<string> alarmCodes, int pageSize, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<IReadOnlyList<AlarmEventModel>> PollKnownAlarmsAsync(
            string windfarmId, string turbineRdspp, string itemId, IReadOnlyList<long> alarmIds, CancellationToken cancellationToken)
        {
            BatchesRequested.Add(alarmIds);
            return Task.FromResult<IReadOnlyList<AlarmEventModel>>(alarmIds.Select(MakeEvent).ToList());
        }

        public Task<IReadOnlyList<AlarmEventModel>> GetAlarmsBetweenDatesAsync(
            string windfarmId, string turbineRdspp, string itemId, DateTime startUtc, DateTime endUtc,
            IReadOnlyList<string> alarmCodes, int pageSize, int offset, CancellationToken cancellationToken)
            => throw new NotSupportedException();
    }
}
