using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using ProductionIngestor.Worker.Configuration;
using ProductionIngestor.Configuration;
using ProductionIngestor.Enums;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;
using ProductionIngestor.Operations.Execution;
using ProductionIngestor.Worker.Operations;
using ProductionIngestor.Monitoring;

namespace ProductionIngestor.UnitTests;

public sealed class ScheduledIngestionTests
{
    [Fact]
    public async Task WorkerRegistersTelemetryWithoutEnablingNetworkExport()
    {
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings { DisableDefaults = true });
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?> { ["Telemetry:ExportEnabled"] = "false" });
        builder.AddProductionTelemetry();
        using var host = builder.Build();
        await host.StartAsync();
        try
        {
            Assert.NotNull(host.Services.GetRequiredService<TracerProvider>());
            Assert.NotNull(host.Services.GetRequiredService<MeterProvider>());
            using var activity = ScheduledIngestionWorker.Activities.StartActivity("test-attempt");
            Assert.NotNull(activity);
            Assert.Equal(System.Diagnostics.ActivityIdFormat.W3C, activity.IdFormat);
        }
        finally { await host.StopAsync(); }
    }

    [Fact]
    public async Task ScheduledModeIgnoresLegacyFarmAndRunsSequentially()
    {
        var fixture = new Fixture();
        var result = await fixture.Service(new() { ["Ingestion:WindfarmId"] = "invalid" }).RunAsync(default);
        Assert.Equal(0, result.ExitCode);
        Assert.Equal(new[] { "1", "2", "3" }, fixture.Executed);
        Assert.Equal(1, fixture.MaximumActive);
    }

    [Fact]
    public async Task FailureDoesNotStopOtherFarms()
    {
        var fixture = new Fixture { FailSecond = true };
        Assert.Equal(1, (await fixture.Service().RunAsync(default)).ExitCode);
        Assert.Equal(3, fixture.Executed.Count);
    }

    [Fact]
    public async Task ClaimBudgetStopsDrainingAndReportsRemainingDue()
    {
        var fixture = new Fixture();
        var result = await fixture.Worker.RunAsync(new(new("Production"), "owner",
            new IngestionOptions { MaxClaimsPerInvocation = 2 }), default);
        Assert.True(result.BudgetExhausted);
        Assert.True(result.Succeeded);
        Assert.Equal(1, result.Selection.DueCount);
        Assert.Equal(2, fixture.Executed.Count);
    }

    [Fact]
    public async Task NoDueWorkIsSuccessfulWithoutClaims()
    {
        var fixture = new Fixture { Busy = true };
        Assert.Equal(0, (await fixture.Service().RunAsync(default)).ExitCode);
        Assert.Empty(fixture.Executed);
    }

    [Fact]
    public async Task CancellationStopsFurtherClaims()
    {
        using var cancellation = new CancellationTokenSource();
        var fixture = new Fixture { Cancellation = cancellation };
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => fixture.Service().RunAsync(cancellation.Token));
        Assert.Single(fixture.Executed);
    }

    [Fact]
    public async Task RefreshUsesSelectedFarmAndDatesWithoutScheduledFarm()
    {
        var fixture = new Fixture();
        Assert.Equal(0, (await fixture.Service(Refresh()).RunAsync(default)).ExitCode);
        Assert.Equal("2", Assert.Single(fixture.Executed));
        Assert.Equal(IngestionMode.Refresh, fixture.LastRequest!.Mode);
        Assert.Equal(2, fixture.LastRequest.ExplicitDates!.Count);
        Assert.Equal("request-1", fixture.LastRequest.RunId);
    }

    [Fact]
    public async Task BusyRefreshReturnsFailure()
    {
        var fixture = new Fixture { Busy = true };
        Assert.Equal(1, (await fixture.Service(Refresh()).RunAsync(default)).ExitCode);
        Assert.Empty(fixture.Executed);
    }

    [Fact]
    public async Task DisabledRefreshFailsValidation()
    {
        var fixture = new Fixture { Enabled = false };
        Assert.Equal(2, (await fixture.Service(Refresh()).RunAsync(default)).ExitCode);
        Assert.Equal(0, fixture.ClaimCount);
    }

    [Theory]
    [InlineData("REFRESH_WINDFARM_ID", "2")]
    [InlineData("REFRESH_DATA_DATES", "2026-09-01")]
    [InlineData("REFRESH_RUN_ID", "request-only")]
    [InlineData("REFRESH_REQUESTED_BY", "operator")]
    [InlineData("REFRESH_REASON", "repair")]
    public async Task PartialRefreshFailsClosed(string key, string value)
    {
        var fixture = new Fixture();
        Assert.Equal(2, (await fixture.Service(new() { [key] = value }).RunAsync(default)).ExitCode);
        Assert.Equal(0, fixture.ClaimCount);
    }

    private static Dictionary<string, string?> Refresh() => new()
    {
        ["REFRESH_WINDFARM_ID"] = "2", ["REFRESH_WINDFARM_NAME"] = "farm-2",
        ["REFRESH_DATA_DATES"] = "2026-09-02,2026-09-01,2026-09-02",
        ["REFRESH_RUN_ID"] = "request-1", ["REFRESH_REQUESTED_BY"] = "operator", ["REFRESH_REASON"] = "repair"
    };

    private sealed class Fixture : IExtractionSchedulingRepository, IProductionAssignmentExecutor
    {
        private int _nextFarm = 1;
        private int _active;
        public int MaximumActive { get; private set; }
        public int ClaimCount { get; private set; }
        public bool Busy { get; init; }
        public bool Enabled { get; init; } = true;
        public bool FailSecond { get; init; }
        public CancellationTokenSource? Cancellation { get; init; }
        public List<string> Executed { get; } = [];
        public IngestionRequestModel? LastRequest { get; private set; }
        public ScheduledIngestionWorker Worker => new(this, this, NullLogger<ScheduledIngestionWorker>.Instance);
        public IngestionExecutionService Service(Dictionary<string, string?>? settings = null)
            => new(new ConfigurationBuilder().AddInMemoryCollection(settings ?? new()).Build(),
                Options.Create(new IngestionOptions()), this, this, Worker,
                new IngestionTelemetry<IngestionExecutionService>(NullLogger<IngestionExecutionService>.Instance));

        public Task<bool> IsRegisteredAsync(ExtractionLookupModel lookup, CancellationToken cancellationToken) => Task.FromResult(Enabled);
        public Task<ExtractionSelectionStatusModel> GetSelectionStatusAsync(ExtractionClaimRequestModel request, CancellationToken cancellationToken)
            => Task.FromResult(new ExtractionSelectionStatusModel(3, Busy ? 3 : 0, Busy ? 0 : 4 - _nextFarm));
        public Task<bool> TryFinalizeAsync(ExtractionAttemptResultModel attempt, CancellationToken cancellationToken) => Task.FromResult(true);

        public Task<ExtractionClaimModel?> TryClaimCursorAndStartExtractionRunAsync(ExtractionClaimRequestModel request, CancellationToken cancellationToken)
        {
            ClaimCount++;
            if (Busy || _nextFarm > 3) return Task.FromResult<ExtractionClaimModel?>(null);
            var ingestion = request.Refresh ?? new IngestionRequestModel(_nextFarm++, $"farm-{_nextFarm - 1}", "Production", IngestionMode.Incremental);
            var initial = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);
            var lease = new ExtractionLeaseModel
            {
                WindfarmId = ingestion.WindfarmId, WindfarmName = ingestion.WindfarmName, SourceType = "Production", LeaseToken = 1,
                LastUpdatedUtc = initial, LastCommittedId = "0", LeaseExpiresAtUtc = initial.AddYears(1)
            };
            return Task.FromResult<ExtractionClaimModel?>(new(ingestion, lease,
                new(1, ingestion.WindfarmId, "Production", ExtractionRunStatus.Running, initial, null, initial, "0", null, null, "raw", null), request.OwnerId));
        }

        public async Task<RunResultModel> ExecuteAsync(ExtractionClaimModel claim, CancellationToken cancellationToken)
        {
            Executed.Add(claim.Request.WindfarmId.ToString());
            LastRequest = claim.Request;
            MaximumActive = Math.Max(MaximumActive, ++_active);
            try
            {
                await Task.Yield();
                Cancellation?.Cancel();
                cancellationToken.ThrowIfCancellationRequested();
                if (FailSecond && claim.Request.WindfarmId == 2) throw new IOException("Source unavailable");
                return new(RunOutcome.Success, []);
            }
            finally { _active--; }
        }
    }
}