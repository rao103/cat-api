using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using OutageIngestor.Configuration;
using OutageIngestor.Enums;
using OutageIngestor.Interfaces;
using OutageIngestor.Models;
using OutageIngestor.Monitoring;
using OutageIngestor.Operations.Execution;
using OutageIngestor.Worker.Operations;

namespace OutageIngestor.UnitTests;

public sealed class WorkerIngestionExecutionTests
{
    private const string ScopeStartUtc = "2026-08-15T03:04:05.1234567Z";
    private const string ScopeEndUtc = "2026-08-17T06:07:08.7654321Z";

    [Fact]
    public async Task LocalRefreshParsesIntegerIdentityAndLogsAuthoritativeLeaseNameWithoutMutatingConfiguration()
    {
        var fixture = new Fixture();

        var result = await fixture.Service.RunRefreshAsync("00042", "incorrect-request", "run-42", ScopeStartUtc, ScopeEndUtc, "caller", "reason", default);

        Assert.Equal(0, result.ExitCode);
        Assert.Equal(42, Assert.Single(fixture.Lookups).WindfarmId);
        var refresh = Assert.Single(fixture.ClaimRequests).Refresh;
        Assert.NotNull(refresh);
        Assert.Equal(42, refresh.WindfarmId);
        Assert.Equal("incorrect-request", refresh.WindfarmName);
        Assert.Equal(IngestionMode.Refresh, refresh.Mode);
        Assert.Equal("caller", fixture.ClaimRequests[0].RequestedBy);
        Assert.Equal("reason", fixture.ClaimRequests[0].Reason);
        var claim = Assert.Single(fixture.Executed);
        Assert.Equal(42, claim.Lease.WindfarmId);
        Assert.Equal("CHW01", claim.Lease.WindfarmName);
        Assert.Equal("CHW01", Assert.Single(fixture.ExecutionLogger.Scopes)["WindfarmName"]);
        AssertExactRefreshScope(fixture);
        Assert.Null(fixture.Configuration["REFRESH_WINDFARM_ID"]);
        Assert.Null(fixture.Configuration["REFRESH_WINDFARM_NAME"]);
        Assert.Null(fixture.Configuration["REFRESH_SCOPE_START_UTC"]);
        Assert.Null(fixture.Configuration["REFRESH_SCOPE_END_UTC"]);
    }

    [Theory]
    [InlineData("CHW01", "incorrect-request")]
    [InlineData("0", "incorrect-request")]
    [InlineData("42", "")]
    [InlineData("42", " ")]
    public async Task InvalidRefreshStopsBeforeRegistrationOrClaim(string id, string name)
    {
        var fixture = new Fixture();
        var result = await fixture.Service.RunRefreshAsync(id, name, "run-42", "2026-09-01T00:00:00Z", "2026-09-02T00:00:00Z", "caller", null, default);
        Assert.Equal(2, result.ExitCode);
        Assert.Empty(fixture.Lookups);
        Assert.Empty(fixture.ClaimRequests);
        Assert.Empty(fixture.Executed);
    }

    [Fact]
    public async Task EnvironmentRefreshUsesTheSameIntegerParsingBoundary()
    {
        var fixture = new Fixture();
        fixture.Configuration["REFRESH_WINDFARM_ID"] = "42";
        fixture.Configuration["REFRESH_WINDFARM_NAME"] = "incorrect-request";
        fixture.Configuration["REFRESH_RUN_ID"] = "run-42";
        fixture.Configuration["REFRESH_SCOPE_START_UTC"] = ScopeStartUtc;
        fixture.Configuration["REFRESH_SCOPE_END_UTC"] = ScopeEndUtc;

        var result = await fixture.Service.RunAsync(default);

        Assert.Equal(0, result.ExitCode);
        Assert.Equal(42, Assert.Single(fixture.Lookups).WindfarmId);
        Assert.Equal("CHW01", Assert.Single(fixture.ExecutionLogger.Scopes)["WindfarmName"]);
        AssertExactRefreshScope(fixture);
    }

    [Theory]
    [InlineData(null, null)]
    [InlineData(null, ScopeEndUtc)]
    [InlineData(ScopeStartUtc, null)]
    [InlineData("", ScopeEndUtc)]
    [InlineData(ScopeStartUtc, " ")]
    [InlineData("not-a-date", ScopeEndUtc)]
    [InlineData(ScopeStartUtc, "not-a-date")]
    [InlineData("2026-09-31T00:00:00Z", ScopeEndUtc)]
    [InlineData(ScopeStartUtc, ScopeStartUtc)]
    [InlineData(ScopeEndUtc, ScopeStartUtc)]
    public async Task InvalidScopeStopsLocalAndEnvironmentRefreshWithoutScheduledFallback(string? startUtc, string? endUtc)
    {
        var local = new Fixture();
        // The listener can receive missing JSON fields despite its non-nullable wire contract.
        var localResult = await local.Service.RunRefreshAsync("42", "CHW01", "run-42", startUtc!, endUtc!, "caller", null, default);
        Assert.Equal(2, localResult.ExitCode);
        Assert.Empty(local.Lookups);
        Assert.Empty(local.ClaimRequests);
        Assert.Empty(local.Executed);

        var environment = new Fixture();
        environment.Configuration["REFRESH_WINDFARM_ID"] = "42";
        environment.Configuration["REFRESH_WINDFARM_NAME"] = "CHW01";
        environment.Configuration["REFRESH_RUN_ID"] = "run-42";
        environment.Configuration["REFRESH_SCOPE_START_UTC"] = startUtc;
        environment.Configuration["REFRESH_SCOPE_END_UTC"] = endUtc;
        var environmentResult = await environment.Service.RunAsync(default);
        Assert.Equal(2, environmentResult.ExitCode);
        Assert.Empty(environment.Lookups);
        Assert.Empty(environment.ClaimRequests);
        Assert.Empty(environment.Executed);
    }

    [Theory]
    [InlineData(ScopeStartUtc, null)]
    [InlineData(null, ScopeEndUtc)]
    [InlineData(ScopeStartUtc, ScopeEndUtc)]
    [InlineData("", null)]
    [InlineData(null, " ")]
    [InlineData("not-a-date", null)]
    [InlineData(null, "not-a-date")]
    public async Task ScopeOnlyEnvironmentNeverFallsBackToScheduledClaim(string? startUtc, string? endUtc)
    {
        var fixture = new Fixture();
        fixture.Configuration["Ingestion:WindfarmId"] = "42";
        fixture.Configuration["REFRESH_SCOPE_START_UTC"] = startUtc;
        fixture.Configuration["REFRESH_SCOPE_END_UTC"] = endUtc;

        var result = await fixture.Service.RunAsync(default);

        Assert.Equal(2, result.ExitCode);
        Assert.Empty(fixture.Lookups);
        Assert.Empty(fixture.ClaimRequests);
        Assert.Empty(fixture.Executed);
    }

    [Fact]
    public async Task NoRefreshSettingsUseScheduledClaimAndDatabaseNameForStructuredLogs()
    {
        var fixture = new Fixture();

        var result = await fixture.Service.RunAsync(default);

        Assert.Equal(0, result.ExitCode);
        Assert.Empty(fixture.Lookups);
        Assert.Null(Assert.Single(fixture.ClaimRequests).Refresh);
        Assert.Equal(42, Assert.Single(fixture.Executed).Request.WindfarmId);
        Assert.Equal("CHW01", Assert.Single(fixture.SchedulingLogger.Scopes)["WindfarmName"]);
        var started = Assert.Single(fixture.SchedulingLogger.Entries, entry => entry.ContainsKey("WindfarmName"));
        Assert.Equal("CHW01", started["WindfarmName"]);
        Assert.False(started.ContainsKey("WindfarmId"));
    }

    [Fact]
    public async Task UnregisteredRefreshDoesNotClaimOrExecute()
    {
        var fixture = new Fixture { Registered = false };
        var result = await fixture.Service.RunRefreshAsync("42", "incorrect-request", "run-42", "2026-09-01T00:00:00Z", "2026-09-02T00:00:00Z", "caller", null, default);
        Assert.Equal(2, result.ExitCode);
        Assert.Equal(42, Assert.Single(fixture.Lookups).WindfarmId);
        Assert.Empty(fixture.ClaimRequests);
        Assert.Empty(fixture.Executed);
    }

    [Fact]
    public async Task BusyRefreshDoesNotExecute()
    {
        var fixture = new Fixture { Busy = true };
        var result = await fixture.Service.RunRefreshAsync("42", "incorrect-request", "run-42", "2026-09-01T00:00:00Z", "2026-09-02T00:00:00Z", "caller", null, default);
        Assert.Equal(1, result.ExitCode);
        Assert.Single(fixture.ClaimRequests);
        Assert.Empty(fixture.Executed);
    }

    private static void AssertExactRefreshScope(Fixture fixture)
    {
        var expectedStart = new DateTime(2026, 8, 15, 3, 4, 5, DateTimeKind.Utc).AddTicks(1234567);
        var expectedEnd = new DateTime(2026, 8, 17, 6, 7, 8, DateTimeKind.Utc).AddTicks(7654321);
        var refresh = Assert.Single(fixture.ClaimRequests).Refresh;
        Assert.NotNull(refresh);
        Assert.Equal(expectedStart, refresh.RefreshScopeStartUtc);
        Assert.Equal(expectedEnd, refresh.RefreshScopeEndUtc);
        var executed = Assert.Single(fixture.Executed);
        Assert.Equal(IngestionMode.Refresh, executed.Request.Mode);
        Assert.Equal("run-42", executed.Request.RunId);
        Assert.Equal(expectedStart, executed.Request.RefreshScopeStartUtc);
        Assert.Equal(expectedEnd, executed.Request.RefreshScopeEndUtc);
        Assert.Equal(DateTimeKind.Utc, executed.Request.RefreshScopeStartUtc!.Value.Kind);
        Assert.Equal(DateTimeKind.Utc, executed.Request.RefreshScopeEndUtc!.Value.Kind);
        Assert.True(expectedEnd < executed.Lease.LastUpdatedUtc);
    }

    private sealed class Fixture : IExtractionSchedulingRepository, IOutageAssignmentExecutor
    {
        public IConfigurationRoot Configuration { get; } = new ConfigurationBuilder().AddInMemoryCollection().Build();
        public TestIngestionLogger<IngestionExecutionService> ExecutionLogger { get; } = new();
        public TestIngestionLogger<ScheduledIngestionWorker> SchedulingLogger { get; } = new();
        public List<ExtractionLookupModel> Lookups { get; } = [];
        public List<ExtractionClaimRequestModel> ClaimRequests { get; } = [];
        public List<ExtractionClaimModel> Executed { get; } = [];
        public bool Registered { get; init; } = true;
        public bool Busy { get; init; }

        public IngestionExecutionService Service => new(Configuration,
            Options.Create(new IngestionOptions { MaxClaimsPerInvocation = 1 }), this, this,
            new ScheduledIngestionWorker(this, this, SchedulingLogger),
            new IngestionTelemetry<IngestionExecutionService>(ExecutionLogger));

        public Task<bool> IsRegisteredAsync(ExtractionLookupModel lookup, CancellationToken cancellationToken)
        {
            Lookups.Add(lookup);
            return Task.FromResult(Registered);
        }

        public Task<ExtractionClaimModel?> TryClaimCursorAndStartExtractionRunAsync(ExtractionClaimRequestModel request, CancellationToken cancellationToken)
        {
            ClaimRequests.Add(request);
            if (Busy)
                return Task.FromResult<ExtractionClaimModel?>(null);
            var start = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);
            // Deliberately retain the wrong request name: consumers must use the lease name.
            var ingestion = request.Refresh ?? new(42, "incorrect-request", "Outage", IngestionMode.Incremental, "scheduled-run");
            var lease = new ExtractionLeaseModel
            {
                WindfarmId = 42, WindfarmName = "CHW01", SourceType = "Outage",
                LastUpdatedUtc = start, LastCommittedId = "OC-live",
                DeletedLastUpdatedUtc = start.AddDays(-1), DeletedLastCommittedId = "OC-deleted",
                LeaseToken = 73, LeaseExpiresAtUtc = DateTime.UtcNow.AddMinutes(5)
            };
            var run = new ExtractionRunModel(99, 42, "Outage", ExtractionRunStatus.Running,
                start, null, start, "OC-live", null, null, null, null);
            return Task.FromResult<ExtractionClaimModel?>(new(ingestion, lease, run, request.OwnerId));
        }

        public Task<RunResultModel> ExecuteAsync(ExtractionClaimModel claim, CancellationToken cancellationToken)
        {
            Executed.Add(claim);
            return Task.FromResult(new RunResultModel(RunOutcome.Success, 0));
        }

        public Task<ExtractionSelectionStatusModel> GetSelectionStatusAsync(ExtractionClaimRequestModel request, CancellationToken cancellationToken)
            => Task.FromResult(new ExtractionSelectionStatusModel(1, 0, 0));

        public Task<bool> TryFinalizeAsync(ExtractionAttemptResultModel attempt, CancellationToken cancellationToken)
            => throw new NotSupportedException("Normal finalization belongs to the runner, not its Worker entry point.");
    }
}