using System.Security.Claims;
using System.Text.Json;
using Centralize.Api.Controllers.v1;
using Centralize.Api.Interfaces;
using Centralize.Core.Interfaces;
using Centralize.Core.Models;
using Centralize.Core.Validators;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging.Abstractions;

namespace Centralize.UnitTests;

public sealed class ProductionApiTests
{
    [Theory]
    [InlineData(null)]
    [InlineData("Production")]
    [InlineData("Outage")]
    [InlineData("Alarm")]
    [InlineData("Curtailment")]
    public async Task Refresh_CreatesOneOperationAndTriggersSelectedSources(string? source)
    {
        var repository = new RecordingRepository();
        var trigger = new RecordingTrigger(repository);
        var controller = Controller(repository, trigger);
        using var cancellation = new CancellationTokenSource();
        var dates = new RefreshDates(new(2026, 9, 1), new(2026, 9, 30));
        var result = source is null ? await controller.PostAll(12, dates, cancellation.Token)
            : await controller.PostSource(12, source, dates, cancellation.Token);
        var accepted = Assert.IsType<AcceptedResult>(result);
        var json = JsonSerializer.SerializeToElement(accepted.Value);
        Assert.Equal(repository.OperationId, json.GetProperty("operationId").GetGuid());
        Assert.False(json.TryGetProperty("status", out _));
        Assert.Equal($"/v1/operations/{repository.OperationId}", accepted.Location);
        var expected = source is null ? OperationRequestValidator.SupportedSources : [source];
        Assert.Equal(expected, trigger.Calls.Select(call => call.Source.SourceType));
        Assert.Equal(1, repository.CreateCount);
        Assert.All(trigger.Calls, call =>
        {
            Assert.Equal(repository.OperationId, call.Operation);
            Assert.Equal("caller", call.Request.RequestedBy);
            Assert.Equal(dates.StartDate, call.Request.StartDate);
            Assert.Equal(dates.EndDate, call.Request.EndDate);
            Assert.Equal(cancellation.Token, call.Token);
        });
        Assert.Equal(trigger.Calls.Count, trigger.Calls.Select(call => call.Run).Distinct().Count());
    }

    [Fact]
    public async Task LegacyRefresh_UsesProductionJobAndKeepsReason()
    {
        var repository = new RecordingRepository();
        var trigger = new RecordingTrigger(repository);
        var result = await Controller(repository, trigger).Post(new()
        {
            WindfarmId = "12", WindfarmName = "ignored-client-name",
            StartDate = new(2026, 9, 1), EndDate = new(2026, 9, 2), Reason = "reconcile"
        }, default);
        Assert.IsType<AcceptedResult>(result);
        var call = Assert.Single(trigger.Calls);
        Assert.Equal("Production", call.Source.SourceType);
        Assert.Equal("registered-farm", call.Source.WindfarmName);
        Assert.Equal("reconcile", call.Request.Reason);
    }

    [Fact]
    public async Task MissingCaller_DoesNotCreateOrTrigger()
    {
        var repository = new RecordingRepository();
        var trigger = new RecordingTrigger(repository);
        var response = await Controller(repository, trigger, false)
            .PostAll(12, new(new(2026, 9, 1), new(2026, 9, 30)), default);
        Assert.Equal(403, Assert.IsType<StatusCodeResult>(response).StatusCode);
        Assert.Equal(0, repository.CreateCount);
        Assert.Empty(trigger.Calls);
    }

    [Theory]
    [InlineData("Adjustment")]
    [InlineData("unknown")]
    public async Task InvalidSource_DoesNotCreateOrTrigger(string source)
    {
        var repository = new RecordingRepository();
        var trigger = new RecordingTrigger(repository);
        Assert.IsType<BadRequestObjectResult>(await Controller(repository, trigger)
            .PostSource(12, source, new(new(2026, 9, 1), new(2026, 9, 2)), default));
        Assert.Equal(0, repository.CreateCount);
        Assert.Empty(trigger.Calls);
    }

    [Fact]
    public async Task MissingRegistration_DoesNotCreateOrTrigger()
    {
        var repository = new RecordingRepository { MissingSource = true };
        var trigger = new RecordingTrigger(repository);
        Assert.IsType<NotFoundObjectResult>(await Controller(repository, trigger)
            .PostAll(12, new(new(2026, 9, 1), new(2026, 9, 2)), default));
        Assert.Equal(0, repository.CreateCount);
        Assert.Empty(trigger.Calls);
    }

    [Fact]
    public async Task InvalidJobConfiguration_DoesNotCreateIdentity()
    {
        var repository = new RecordingRepository();
        var trigger = new RecordingTrigger(repository) { InvalidConfiguration = true };
        await Assert.ThrowsAsync<InvalidOperationException>(() => Controller(repository, trigger)
            .PostAll(12, new(new(2026, 9, 1), new(2026, 9, 2)), default));
        Assert.Equal(0, repository.CreateCount);
        Assert.Empty(trigger.Calls);
    }

    [Fact]
    public async Task FailedPersistence_DoesNotTriggerJobs()
    {
        var repository = new RecordingRepository { FailCreate = true };
        var trigger = new RecordingTrigger(repository);
        await Assert.ThrowsAsync<InvalidOperationException>(() => Controller(repository, trigger)
            .PostAll(12, new(new(2026, 9, 1), new(2026, 9, 2)), default));
        Assert.Empty(trigger.Calls);
    }

    [Theory]
    [InlineData("Outage")]
    [InlineData("*")]
    public async Task SubmissionFailure_ReturnsOperationAndEachOutcomeWithoutFalseAcceptance(string failure)
    {
        var repository = new RecordingRepository();
        var trigger = new RecordingTrigger(repository) { Failure = failure };
        var result = Assert.IsType<ObjectResult>(await Controller(repository, trigger)
            .PostAll(12, new(new(2026, 9, 1), new(2026, 9, 2)), default));
        Assert.Equal(502, result.StatusCode);
        var json = JsonSerializer.SerializeToElement(result.Value);
        Assert.Equal(repository.OperationId, json.GetProperty("operationId").GetGuid());
        var jobs = json.GetProperty("jobs").EnumerateArray().ToArray();
        Assert.Equal(4, jobs.Length);
        Assert.Equal(failure == "*" ? 4 : 1, jobs.Count(job => job.GetProperty("Error").ValueKind == JsonValueKind.String));
        Assert.DoesNotContain("secret detail", json.ToString());
    }

    [Fact]
    public async Task Cancellation_IsPropagatedWithoutReportingAcceptance()
    {
        var repository = new RecordingRepository();
        var trigger = new RecordingTrigger(repository);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Controller(repository, trigger)
            .PostAll(12, new(new(2026, 9, 1), new(2026, 9, 2)), cancellation.Token));
        Assert.Equal(0, repository.CreateCount);
        Assert.Empty(trigger.Calls);
    }

    [Fact]
    public async Task Monitoring_IsScopedToCallerAndDoesNotInventProgress()
    {
        var repository = new RecordingRepository();
        var controller = WithCaller(new OperationsController(repository), true);
        var operation = Assert.IsType<OperationModel>(Assert.IsType<OkObjectResult>(
            await controller.Get(repository.OperationId, default)).Value);
        Assert.Equal("caller", repository.LookupCaller);
        Assert.Empty(operation.Runs);
        Assert.Empty(operation.Stages);
    }

    [Fact]
    public async Task Monitoring_ReturnsNotFoundForUnownedOperation()
    {
        var repository = new RecordingRepository { Missing = true };
        Assert.IsType<NotFoundResult>(await WithCaller(new OperationsController(repository), true)
            .Get(repository.OperationId, default));
    }

    private static RefreshController Controller(RecordingRepository repository, RecordingTrigger trigger, bool caller = true) =>
        WithCaller(new RefreshController(repository, trigger, NullLogger<RefreshController>.Instance), caller);

    private static T WithCaller<T>(T controller, bool caller) where T : ControllerBase
    {
        controller.ControllerContext = new() { HttpContext = new DefaultHttpContext
        {
            User = new ClaimsPrincipal(new ClaimsIdentity(caller ? new[] { new Claim("oid", "caller") } : [], "unit"))
        }};
        return controller;
    }

    private sealed class RecordingRepository : IOperationRepository
    {
        public Guid OperationId { get; } = Guid.NewGuid();
        public int CreateCount { get; private set; }
        public string? LookupCaller { get; private set; }
        public bool MissingSource { get; init; }
        public bool FailCreate { get; init; }
        public bool Missing { get; init; }
        public Task<IReadOnlyList<RefreshSourceModel>> GetRefreshSourcesAsync(OperationRequestModel request, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            if (MissingSource) throw new KeyNotFoundException("No source");
            return Task.FromResult<IReadOnlyList<RefreshSourceModel>>((request.SourceType is null
                ? OperationRequestValidator.SupportedSources : new[] { request.SourceType })
                .Select(source => new RefreshSourceModel(request.WindfarmId, "registered-farm", source)).ToArray());
        }
        public Task<OperationModel> CreateAsync(string caller, CancellationToken token)
        {
            token.ThrowIfCancellationRequested();
            if (FailCreate) throw new InvalidOperationException("Commit failed");
            CreateCount++;
            return Task.FromResult(Result());
        }
        public Task<OperationModel?> GetAsync(Guid id, string caller, CancellationToken token)
        {
            LookupCaller = caller;
            return Task.FromResult<OperationModel?>(Missing ? null : Result());
        }
        public Task<IReadOnlyList<RefreshAttempt>> GetRefreshAttemptsAsync(string id, CancellationToken token) =>
            Task.FromResult<IReadOnlyList<RefreshAttempt>>([]);
        private OperationModel Result() => new(OperationId, DateTimeOffset.UtcNow, "caller", [], []);
    }

    private sealed class RecordingTrigger(RecordingRepository repository) : IIngestionJobTrigger
    {
        public List<(OperationRequestModel Request, RefreshSourceModel Source, Guid Operation, string Run, CancellationToken Token)> Calls { get; } = [];
        public bool InvalidConfiguration { get; init; }
        public string? Failure { get; init; }
        public void ValidateSources(IReadOnlyList<RefreshSourceModel> sources)
        {
            Assert.Equal(0, repository.CreateCount);
            if (InvalidConfiguration) throw new InvalidOperationException("Invalid configuration");
        }
        public Task<string> TriggerAsync(OperationRequestModel request, RefreshSourceModel source, Guid operationId, string runId, CancellationToken token)
        {
            Assert.Equal(1, repository.CreateCount);
            token.ThrowIfCancellationRequested();
            Calls.Add((request, source, operationId, runId, token));
            if (Failure == "*" || Failure == source.SourceType) throw new HttpRequestException("secret detail");
            return Task.FromResult(source.SourceType.ToLowerInvariant() + "-refresh-" + runId);
        }
    }
}
