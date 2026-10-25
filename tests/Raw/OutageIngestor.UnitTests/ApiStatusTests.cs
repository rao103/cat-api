using System.Net;
using System.Security.Claims;
using k8s;
using k8s.Models;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using OutageIngestor.Api.Configuration;
using OutageIngestor.Api.Controllers.v1;
using OutageIngestor.Api.Results;
using OutageIngestor.Api.Triggering;
using OutageIngestor.Interfaces;
using OutageIngestor.Models;

namespace OutageIngestor.UnitTests;

public sealed class ApiStatusTests
{
    [Theory]
    [InlineData("not-an-id")]
    [InlineData("0")]
    [InlineData("-1")]
    [InlineData("2147483648")]
    public async Task Cursor_InvalidId_ReturnsNotFoundWithoutQuery(string windfarmId)
    {
        var telemetry = new ApiTestTelemetry<StatusController>();
        var controller = Controller(ApiInterfaceProxy<IExtractionCursorRepository>.Create((_, _) =>
            throw new InvalidOperationException("Must not query")), telemetry);

        Assert.Equal(404, (await controller.Get(windfarmId, "Outage", default)).StatusCode);
        Assert.Equal("caller", Assert.Single(telemetry.Errors).CallerId);
    }

    [Fact]
    public async Task Cursor_QueriesNumericIdentityAndSource_WithCancellation()
    {
        using var cancellation = new CancellationTokenSource();
        var current = new ExtractionLeaseModel
        {
            WindfarmId = 42, WindfarmName = "CHW01", SourceType = "Outage",
            LastUpdatedUtc = DateTime.UtcNow, LastCommittedId = "1",
            DeletedLastUpdatedUtc = DateTime.UtcNow, DeletedLastCommittedId = "2",
            LeaseToken = 3, LeaseExpiresAtUtc = null
        };
        var repository = ApiInterfaceProxy<IExtractionCursorRepository>.Create((method, args) =>
        {
            Assert.Equal(nameof(IExtractionCursorRepository.GetCurrentAsync), method.Name);
            Assert.Equal(42, args[0]);
            Assert.Equal("Outage", args[1]);
            Assert.Equal(cancellation.Token, args[2]);
            return Task.FromResult<ExtractionLeaseModel?>(current);
        });
        var telemetry = new ApiTestTelemetry<StatusController>();

        var response = await Controller(repository, telemetry).Get("42", "Outage", cancellation.Token);

        Assert.Same(current, Assert.IsType<Result>(response.Value).Object);
        Assert.Equal("caller", Assert.Single(telemetry.InformationEvents).CallerId);
    }

    [Fact]
    public async Task Cursor_Missing_ReturnsNotFound()
    {
        var repository = ApiInterfaceProxy<IExtractionCursorRepository>.Create((_, _) => Task.FromResult<ExtractionLeaseModel?>(null));
        var telemetry = new ApiTestTelemetry<StatusController>();
        Assert.Equal(404, (await Controller(repository, telemetry).Get("42", "Outage", default)).StatusCode);
        Assert.IsType<KeyNotFoundException>(Assert.Single(telemetry.Errors).Error);
    }

    [Fact]
    public async Task Cursor_RepositoryFailure_IsLoggedAndRethrown()
    {
        var error = new InvalidOperationException("database unavailable");
        var repository = ApiInterfaceProxy<IExtractionCursorRepository>.Create((_, _) => Task.FromException<ExtractionLeaseModel?>(error));
        var telemetry = new ApiTestTelemetry<StatusController>();
        Assert.Same(error, await Assert.ThrowsAsync<InvalidOperationException>(() => Controller(repository, telemetry).Get("42", "Outage", default)));
        Assert.Same(error, Assert.Single(telemetry.Errors).Error);
    }

    [Fact]
    public async Task Cursor_ClientCancellation_IsNotLoggedAsFailure()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var repository = ApiInterfaceProxy<IExtractionCursorRepository>.Create((_, _) => Task.FromCanceled<ExtractionLeaseModel?>(cancellation.Token));
        var telemetry = new ApiTestTelemetry<StatusController>();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Controller(repository, telemetry).Get("42", "Outage", cancellation.Token));
        Assert.Empty(telemetry.Errors);
    }

    [Theory]
    [InlineData("Completed", false, "IngestionSucceeded")]
    [InlineData("Running", true, "Running")]
    [InlineData("Running", false, "LeaseExpired")]
    [InlineData("Failed", false, "Failed")]
    [InlineData("Cancelled", false, "Cancelled")]
    [InlineData("Busy", false, "Busy")]
    public void ResolveStatus_UsesLatestDurableAttempt(string status, bool activeLease, string expected) =>
        Assert.Equal(expected, RefreshStatusService.ResolveStatus(null, [Attempt(status, activeLease)]));

    [Fact]
    public void ResolveStatus_DistinguishesStartupFailureAndIngestionFailure()
    {
        var job = new V1Job { Status = new V1JobStatus { Conditions = [new V1JobCondition { Type = "Failed", Status = "True" }] } };
        Assert.Equal("StartupFailed", RefreshStatusService.ResolveStatus(job, []));
        Assert.Equal("Failed", RefreshStatusService.ResolveStatus(job, [Attempt("Running")]));
        Assert.Equal("IngestionSucceeded", RefreshStatusService.ResolveStatus(job, [Attempt("Completed")]));
    }

    [Fact]
    public void ResolveStatus_NeverTreatsSuccessfulJobAsVerifiedIngestion()
    {
        Assert.Equal("Unverified", RefreshStatusService.ResolveStatus(new V1Job { Status = new V1JobStatus { Succeeded = 1 } }, []));
        Assert.Equal("Unknown", RefreshStatusService.ResolveStatus(null, []));
        Assert.Equal("Starting", RefreshStatusService.ResolveStatus(new V1Job { Status = new V1JobStatus { Active = 1 } }, []));
        Assert.Equal("Accepted", RefreshStatusService.ResolveStatus(new V1Job(), []));
    }

    [Fact]
    public async Task Request_LocalMode_UsesHistoryWithoutKubernetes_AndReturnsUnverifiedDownstreamStatus()
    {
        using var cancellation = new CancellationTokenSource();
        var attempt = Attempt("Completed");
        var history = ApiInterfaceProxy<IExtractionRunHistoryRepository>.Create((_, args) =>
        {
            Assert.Equal("run-1", Assert.IsType<ExtractionHistoryQueryModel>(args[0]).RequestId);
            Assert.Equal(cancellation.Token, args[1]);
            return Task.FromResult<IReadOnlyList<ExtractionRunHistoryModel>>([attempt]);
        });
        var service = Service(null, history, Environments.Development, local: true);
        var telemetry = new ApiTestTelemetry<StatusController>();
        var controller = Controller(ApiInterfaceProxy<IExtractionCursorRepository>.Create((_, _) => throw new NotSupportedException()), telemetry);

        var response = Assert.IsType<OkObjectResult>(await controller.GetRequest("run-1", service, cancellation.Token));
        var status = Assert.IsType<RefreshStatusResponse>(response.Value);
        Assert.Equal("local-refresh-run-1", status.JobName);
        Assert.Equal("IngestionSucceeded", status.Status);
        Assert.False(status.EndToEndCompletionVerified);
        Assert.Same(attempt, Assert.Single(status.Attempts));
        Assert.Equal("caller", Assert.Single(telemetry.InformationEvents).CallerId);
    }

    [Fact]
    public async Task Request_NonDevelopment_IgnoresLocalFlag_AndHandlesMissingKubernetesJob()
    {
        using var handler = new ApiHttpHandler((request, _) =>
        {
            Assert.EndsWith("/namespaces/test/jobs/outage-ingestor-refresh-run-1", request.RequestUri!.AbsolutePath);
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NotFound)
            {
                Content = new StringContent("{\"kind\":\"Status\",\"apiVersion\":\"v1\",\"status\":\"Failure\",\"reason\":\"NotFound\",\"code\":404}")
            });
        });
        using var client = new Kubernetes(new KubernetesClientConfiguration { Host = "http://localhost" }, new ApiDelegatingHandler(handler));
        var history = ApiInterfaceProxy<IExtractionRunHistoryRepository>.Create((_, _) => Task.FromResult<IReadOnlyList<ExtractionRunHistoryModel>>([]));

        var status = await Service(client, history, Environments.Production, local: true).GetAsync(new("run-1"), default);

        Assert.Equal("outage-ingestor-refresh-run-1", status.JobName);
        Assert.Equal("Unknown", status.Status);
    }

    [Fact]
    public async Task Request_InvalidJobIdentity_ReturnsBadRequest()
    {
        var history = ApiInterfaceProxy<IExtractionRunHistoryRepository>.Create((_, _) => Task.FromResult<IReadOnlyList<ExtractionRunHistoryModel>>([]));
        var service = Service(null, history, Environments.Production, local: false);
        var telemetry = new ApiTestTelemetry<StatusController>();
        var controller = Controller(ApiInterfaceProxy<IExtractionCursorRepository>.Create((_, _) => throw new NotSupportedException()), telemetry);
        Assert.IsType<BadRequestObjectResult>(await controller.GetRequest("bad/name", service, default));
        Assert.IsType<ArgumentException>(Assert.Single(telemetry.Errors).Error);
    }

    private static StatusController Controller(IExtractionCursorRepository repository, ApiTestTelemetry<StatusController> telemetry) =>
        new(repository, telemetry)
        {
            ControllerContext = new ControllerContext
            {
                HttpContext = new DefaultHttpContext { User = new ClaimsPrincipal(new ClaimsIdentity([new Claim("oid", "caller")], "test")) }
            }
        };

    private static ExtractionRunHistoryModel Attempt(string status, bool activeLease = false) =>
        new(1, "run-1", 42, "Refresh", status, DateTimeOffset.UtcNow.AddMinutes(-5), null, null, null,
            activeLease ? DateTimeOffset.UtcNow.AddHours(1) : DateTimeOffset.UtcNow.AddHours(-1));

    private static RefreshStatusService Service(IKubernetes? client, IExtractionRunHistoryRepository history, string environment, bool local) =>
        new(client, Options.Create(new JobTriggerOptions { Namespace = "test", CronJobName = "outage-ingestor" }), history,
            new ApiHostEnvironment { EnvironmentName = environment },
            new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?> { ["LocalRefresh:Enabled"] = local.ToString() }).Build());

    private sealed class ApiDelegatingHandler(HttpMessageHandler handler) : DelegatingHandler
    {
        private readonly HttpMessageInvoker _invoker = new(handler);
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            _invoker.SendAsync(request, cancellationToken);

        protected override void Dispose(bool disposing)
        {
            if (disposing) _invoker.Dispose();
            base.Dispose(disposing);
        }
    }
}