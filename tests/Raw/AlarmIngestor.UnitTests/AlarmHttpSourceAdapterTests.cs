using System.Net;
using System.Text.Json;
using AlarmIngestor.Adapters.Http;

namespace AlarmIngestor.UnitTests;

/// <summary>
/// <see cref="AlarmHttpSourceAdapter"/> against a fake <see cref="HttpMessageHandler"/> (not just
/// an IAlarmSourceAdapter fake): proves the adapter itself issues exactly one bounded HTTP call
/// per <c>DiscoverPageAsync</c>/<c>GetMaxAlarmIdAsync</c>, and bounds <c>PollKnownAlarmsAsync</c>
/// strictly to the requested ID set across real request/response round-trips.
/// </summary>
public sealed class AlarmHttpSourceAdapterTests
{
    private static readonly DateTime MinRetrievalDate = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private static string AlarmJson(long id, string wtgRdsppId = "RDSPP-1") =>
        JsonSerializer.Serialize(new
        {
            id,
            wtgRdsppId,
            timeOnUtc = "2026-01-01T01:00:00Z",
            alarmCode = "100",
            alarmText = "text",
            alarmType = "Alarm",
            alarmState = "Good",
        });

    [Fact]
    public async Task DiscoverPageAsyncIssuesExactlyOneRequestPerCall()
    {
        var responses = new Queue<string>();
        responses.Enqueue($"[{AlarmJson(1)},{AlarmJson(2)}]");
        var handler = new QueueingHandler(responses, failOnExhausted: true);
        var adapter = new AlarmHttpSourceAdapter(new HttpClient(handler) { BaseAddress = new Uri("https://example.test/") });

        var page = await adapter.DiscoverPageAsync(
            "farm", "RDSPP-1", "ITEM-1", afterAlarmId: 0, upperBoundAlarmId: 100,
            MinRetrievalDate, ["100"], pageSize: 2, CancellationToken.None);

        Assert.Equal(2, page.Count);
        Assert.Equal(1, handler.CallCount);
    }

    [Fact]
    public async Task DiscoverPageAsyncDoesNotLoopWhenPageIsFull()
    {
        // A full page (Count == pageSize) must not trigger a second offset-based request — the
        // reader owns keyset paging, the adapter issues exactly one call (Section "reader
        // controls keyset").
        var responses = new Queue<string>();
        responses.Enqueue($"[{AlarmJson(1)},{AlarmJson(2)}]");
        var handler = new QueueingHandler(responses, failOnExhausted: true);
        var adapter = new AlarmHttpSourceAdapter(new HttpClient(handler) { BaseAddress = new Uri("https://example.test/") });

        var page = await adapter.DiscoverPageAsync(
            "farm", "RDSPP-1", "ITEM-1", afterAlarmId: 0, upperBoundAlarmId: 100,
            MinRetrievalDate, ["100"], pageSize: 2, CancellationToken.None);

        Assert.Equal(2, page.Count);
        Assert.Equal(1, handler.CallCount);
    }

    [Fact]
    public async Task GetMaxAlarmIdAsyncThrowsWhenTop1ResponseReturnsMoreThanOneRow()
    {
        var responses = new Queue<string>();
        responses.Enqueue($"[{AlarmJson(5)},{AlarmJson(4)}]");
        var handler = new QueueingHandler(responses, failOnExhausted: true);
        var adapter = new AlarmHttpSourceAdapter(new HttpClient(handler) { BaseAddress = new Uri("https://example.test/") });

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            adapter.GetMaxAlarmIdAsync("RDSPP-1", MinRetrievalDate, ["100"], CancellationToken.None));
    }

    [Fact]
    public async Task PollKnownAlarmsAsyncPaginatesAcrossMoreThanFiveHundredIds()
    {
        var alarmIds = Enumerable.Range(1, 700).Select(i => (long)i).ToList();
        var firstPage = string.Join(",", alarmIds.Take(500).Select(id => AlarmJson(id)));
        var secondPage = string.Join(",", alarmIds.Skip(500).Take(200).Select(id => AlarmJson(id)));

        var responses = new Queue<string>();
        responses.Enqueue($"[{firstPage}]");
        responses.Enqueue($"[{secondPage}]");
        var handler = new QueueingHandler(responses, failOnExhausted: true);
        var adapter = new AlarmHttpSourceAdapter(new HttpClient(handler) { BaseAddress = new Uri("https://example.test/") });

        var result = await adapter.PollKnownAlarmsAsync("farm", "RDSPP-1", "ITEM-1", alarmIds, CancellationToken.None);

        Assert.Equal(700, result.Count);
        Assert.Equal(2, handler.CallCount);
    }

    [Fact]
    public async Task PollKnownAlarmsAsyncThrowsOnRepeatedFullPage()
    {
        var alarmIds = Enumerable.Range(1, 700).Select(i => (long)i).ToList();
        var firstPage = string.Join(",", alarmIds.Take(500).Select(id => AlarmJson(id)));

        var responses = new Queue<string>();
        responses.Enqueue($"[{firstPage}]");
        responses.Enqueue($"[{firstPage}]"); // server ignores offset and repeats the same full page
        var handler = new QueueingHandler(responses, failOnExhausted: true);
        var adapter = new AlarmHttpSourceAdapter(new HttpClient(handler) { BaseAddress = new Uri("https://example.test/") });

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            adapter.PollKnownAlarmsAsync("farm", "RDSPP-1", "ITEM-1", alarmIds, CancellationToken.None));

        Assert.Equal(2, handler.CallCount);
    }

    [Fact]
    public async Task PollKnownAlarmsAsyncThrowsOnAlarmIdOutsideRequestedSet()
    {
        var alarmIds = new List<long> { 1, 2 };
        var responses = new Queue<string>();
        responses.Enqueue($"[{AlarmJson(1)},{AlarmJson(999)}]");
        var handler = new QueueingHandler(responses, failOnExhausted: true);
        var adapter = new AlarmHttpSourceAdapter(new HttpClient(handler) { BaseAddress = new Uri("https://example.test/") });

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            adapter.PollKnownAlarmsAsync("farm", "RDSPP-1", "ITEM-1", alarmIds, CancellationToken.None));
    }

    /// <summary>Returns queued responses in order; throws on a request beyond the queued set.</summary>
    private sealed class QueueingHandler : HttpMessageHandler
    {
        private readonly Queue<string> _responses;
        private readonly bool _failOnExhausted;

        public int CallCount { get; private set; }

        public QueueingHandler(Queue<string> responses, bool failOnExhausted)
        {
            _responses = responses;
            _failOnExhausted = failOnExhausted;
        }

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            CallCount++;
            if (_responses.Count == 0)
            {
                if (_failOnExhausted)
                {
                    throw new InvalidOperationException("Unexpected additional HTTP request beyond the fake's queued responses.");
                }

                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("[]") });
            }

            var body = _responses.Dequeue();
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body) });
        }
    }
}
