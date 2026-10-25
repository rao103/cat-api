using AlarmIngestor.Configuration;
using AlarmIngestor.Interfaces;
using AlarmIngestor.Models;
using AlarmIngestor.Operations.Discovery;

namespace AlarmIngestor.UnitTests;

/// <summary>
/// <see cref="AlarmDiscoveryReader"/>: the caller freezes the upper AlarmId bound (this reader
/// never calls back into the source to (re)establish it), pages until it reaches that bound, and
/// fails closed on a non-progressing/repeated page instead of looping or silently truncating.
/// </summary>
public sealed class AlarmDiscoveryReaderTests
{
    private static readonly DateTime MinRetrievalDate = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly TurbineIdentity Turbine = new("RDSPP-1", "ITEM-1");

    private static AlarmEventModel MakeEvent(long alarmId) => new(
        "farm", "RDSPP-1", "ITEM-1", alarmId, "100", "text", MinRetrievalDate.AddHours(1), null,
        "Alarm", "Good", false, MinRetrievalDate.AddHours(2), 0);

    [Fact]
    public async Task StopsAtFrozenUpperBoundAcrossMultiplePages()
    {
        var adapter = new FakeAdapter
        {
            Pages = [[MakeEvent(1), MakeEvent(2)], [MakeEvent(3), MakeEvent(4)], [MakeEvent(5)]],
            PageSize = 2,
        };
        var reader = new AlarmDiscoveryReader(adapter);

        var result = new List<AlarmEventModel>();
        await foreach (var page in reader.DiscoverTurbinePagesAsync("farm", Turbine, 0, upperBound: 5, MinRetrievalDate, ["100"], 2, CancellationToken.None))
        {
            result.AddRange(page);
        }

        Assert.Equal(3, adapter.CallCount);
        Assert.Equal(5, result.Count);
    }

    [Fact]
    public async Task ReturnsEmptyWhenNoUpperBoundEstablished()
    {
        var adapter = new FakeAdapter();
        var reader = new AlarmDiscoveryReader(adapter);

        var result = new List<AlarmEventModel>();
        await foreach (var page in reader.DiscoverTurbinePagesAsync("farm", Turbine, 10, upperBound: null, MinRetrievalDate, ["100"], 100, CancellationToken.None))
        {
            result.AddRange(page);
        }

        Assert.Empty(result);
        Assert.Equal(0, adapter.CallCount);
    }

    [Fact]
    public async Task ReturnsEmptyWhenUpperBoundIsAtOrBelowCursor()
    {
        var adapter = new FakeAdapter();
        var reader = new AlarmDiscoveryReader(adapter);

        var result = new List<AlarmEventModel>();
        await foreach (var page in reader.DiscoverTurbinePagesAsync("farm", Turbine, 10, upperBound: 10, MinRetrievalDate, ["100"], 100, CancellationToken.None))
        {
            result.AddRange(page);
        }

        Assert.Empty(result);
        Assert.Equal(0, adapter.CallCount);
    }

    [Fact]
    public async Task NonProgressingPageFailsClosedInsteadOfLooping()
    {
        var adapter = new FakeAdapter
        {
            Pages = [[MakeEvent(1)]],
            RepeatFirstPageForever = true,
        };
        var reader = new AlarmDiscoveryReader(adapter);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in reader.DiscoverTurbinePagesAsync("farm", Turbine, 1, upperBound: 5, MinRetrievalDate, ["100"], 100, CancellationToken.None))
            {
            }
        });
    }

    [Fact]
    public async Task ThrowsBeforeYieldingWhenPageContainsDuplicateId()
    {
        var adapter = new FakeAdapter { Pages = [[MakeEvent(2), MakeEvent(2)]], PageSize = 2 };
        var reader = new AlarmDiscoveryReader(adapter);

        var yielded = new List<AlarmEventModel>();
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var page in reader.DiscoverTurbinePagesAsync("farm", Turbine, 0, upperBound: 5, MinRetrievalDate, ["100"], 2, CancellationToken.None))
            {
                yielded.AddRange(page);
            }
        });

        Assert.Empty(yielded);
    }

    [Fact]
    public async Task ThrowsBeforeYieldingWhenPageIsUnordered()
    {
        var adapter = new FakeAdapter { Pages = [[MakeEvent(3), MakeEvent(2)]], PageSize = 2 };
        var reader = new AlarmDiscoveryReader(adapter);

        var yielded = new List<AlarmEventModel>();
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var page in reader.DiscoverTurbinePagesAsync("farm", Turbine, 0, upperBound: 5, MinRetrievalDate, ["100"], 2, CancellationToken.None))
            {
                yielded.AddRange(page);
            }
        });

        Assert.Empty(yielded);
    }

    [Fact]
    public async Task ThrowsWhenPageIdIsAtOrBelowCursor()
    {
        var adapter = new FakeAdapter { Pages = [[MakeEvent(1)]], PageSize = 2 };
        var reader = new AlarmDiscoveryReader(adapter);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in reader.DiscoverTurbinePagesAsync("farm", Turbine, 2, upperBound: 5, MinRetrievalDate, ["100"], 2, CancellationToken.None))
            {
            }
        });
    }

    [Fact]
    public async Task ThrowsWhenPageIdExceedsFrozenUpperBound()
    {
        var adapter = new FakeAdapter { Pages = [[MakeEvent(6)]], PageSize = 2 };
        var reader = new AlarmDiscoveryReader(adapter);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in reader.DiscoverTurbinePagesAsync("farm", Turbine, 0, upperBound: 5, MinRetrievalDate, ["100"], 2, CancellationToken.None))
            {
            }
        });
    }

    [Fact]
    public async Task ThrowsWhenPageExceedsRequestedPageSize()
    {
        var adapter = new FakeAdapter { Pages = [[MakeEvent(1), MakeEvent(2), MakeEvent(3)]], PageSize = 2 };
        var reader = new AlarmDiscoveryReader(adapter);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in reader.DiscoverTurbinePagesAsync("farm", Turbine, 0, upperBound: 5, MinRetrievalDate, ["100"], 2, CancellationToken.None))
            {
            }
        });
    }

    [Fact]
    public async Task EmptyPageBelowFrozenGlobalUpperBoundStopsCleanlyInsteadOfThrowing()
    {
        // The frozen bound is a GLOBAL cutoff (max across ALL configured turbines), not this
        // turbine's own max, so this turbine legitimately having no rows up to it is not a gap.
        var adapter = new FakeAdapter { Pages = [], PageSize = 2 };
        var reader = new AlarmDiscoveryReader(adapter);

        var result = new List<AlarmEventModel>();
        await foreach (var page in reader.DiscoverTurbinePagesAsync("farm", Turbine, 0, upperBound: 5, MinRetrievalDate, ["100"], 2, CancellationToken.None))
        {
            result.AddRange(page);
        }

        Assert.Empty(result);
        Assert.Equal(1, adapter.CallCount);
    }

    private sealed class FakeAdapter : IAlarmSourceAdapter
    {
        public List<IReadOnlyList<AlarmEventModel>> Pages { get; init; } = [];
        public int PageSize { get; init; } = 100;
        public bool RepeatFirstPageForever { get; init; }
        public int CallCount { get; private set; }

        public Task<long?> GetMaxAlarmIdAsync(string turbineRdspp, DateTime minRetrievalDateUtc, IReadOnlyList<string> alarmCodes, CancellationToken cancellationToken)
            => throw new NotSupportedException("The runner freezes the bound, not the reader.");

        public Task<IReadOnlyList<AlarmEventModel>> DiscoverPageAsync(
            string windfarmId, string turbineRdspp, string itemId, long afterAlarmId, long upperBoundAlarmId,
            DateTime minRetrievalDateUtc, IReadOnlyList<string> alarmCodes, int pageSize, CancellationToken cancellationToken)
        {
            if (RepeatFirstPageForever)
            {
                CallCount++;
                return Task.FromResult(Pages[0]);
            }

            var index = CallCount++;
            return Task.FromResult(index < Pages.Count ? Pages[index] : (IReadOnlyList<AlarmEventModel>)[]);
        }

        public Task<IReadOnlyList<AlarmEventModel>> PollKnownAlarmsAsync(string windfarmId, string turbineRdspp, string itemId, IReadOnlyList<long> alarmIds, CancellationToken cancellationToken)
            => throw new NotSupportedException();

        public Task<IReadOnlyList<AlarmEventModel>> GetAlarmsBetweenDatesAsync(
            string windfarmId, string turbineRdspp, string itemId, DateTime startUtc, DateTime endUtc,
            IReadOnlyList<string> alarmCodes, int pageSize, int offset, CancellationToken cancellationToken)
            => throw new NotSupportedException();
    }
}
