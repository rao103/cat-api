using System.Net;
using System.Security.Claims;
using System.Text.Json;
using k8s.Autorest;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using OutageIngestor.Api.Contracts;
using OutageIngestor.Api.Controllers.v1;
using OutageIngestor.Api.Interfaces;
using OutageIngestor.Api.Results;
using OutageIngestor.Models;
using OutageIngestor.Operations.Refresh;

namespace OutageIngestor.UnitTests;

public sealed class ApiRefreshControllerTests
{
    [Theory]
    [InlineData("oid")]
    [InlineData("http://schemas.microsoft.com/identity/claims/objectidentifier")]
    public async Task Post_UsesCallerOid_GeneratesRunId_AndReturnsAcceptedStatusLink(string oidClaim)
    {
        RefreshRequestModel? captured = null;
        using var cancellation = new CancellationTokenSource();
        var trigger = ApiInterfaceProxy<IIngestionJobTrigger>.Create((method, args) =>
        {
            Assert.Equal(nameof(IIngestionJobTrigger.TriggerAsync), method.Name);
            captured = Assert.IsType<RefreshRequestModel>(args[0]);
            Assert.Equal(cancellation.Token, args[1]);
            return Task.FromResult("refresh-job");
        });
        var telemetry = new ApiTestTelemetry<RefreshController>();
        var controller = CreateController(trigger, telemetry, new Claim(oidClaim, "caller-oid"));
        // Untrusted audit/identity fields from older clients must not become authoritative.
        var request = JsonSerializer.Deserialize<RefreshRequestDto>("""
            {"WindfarmId":"42","WindfarmName":"CHW01","Reason":"reconcile",
             "StartDateUtc":"2026-09-01T03:04:05.1234567Z","EndDateUtc":"2026-09-02T06:07:08.7654321Z",
             "RequestedBy":"forged-caller","RunId":"forged-run"}
            """)!;

        var response = await controller.Post(request, cancellation.Token);

        Assert.Equal(202, response.StatusCode);
        Assert.NotNull(captured);
        Assert.Equal(42, captured.WindfarmId);
        Assert.Equal("CHW01", captured.WindfarmName);
        Assert.Equal("caller-oid", captured.RequestedBy);
        Assert.Equal(new DateTime(2026, 9, 1, 3, 4, 5, DateTimeKind.Utc).AddTicks(1234567), captured.ScopeStartUtc);
        Assert.Equal(new DateTime(2026, 9, 2, 6, 7, 8, DateTimeKind.Utc).AddTicks(7654321), captured.ScopeEndUtc);
        Assert.Equal(DateTimeKind.Utc, captured.ScopeStartUtc.Kind);
        Assert.Equal(DateTimeKind.Utc, captured.ScopeEndUtc.Kind);
        Assert.True(Guid.TryParseExact(captured.RunId, "N", out _));
        var result = Assert.IsType<Result>(response.Value);
        Assert.True(result.Success);
        var json = JsonSerializer.SerializeToElement(result.Object);
        Assert.Equal(captured.RunId, json.GetProperty("RunId").GetString());
        Assert.Equal($"/v1/status/requests/{captured.RunId}", json.GetProperty("StatusUrl").GetString());
        Assert.Equal("refresh-job", json.GetProperty("JobName").GetString());
        Assert.Equal("caller-oid", Assert.Single(telemetry.InformationEvents).CallerId);
        Assert.Empty(telemetry.Errors);
    }

    [Fact]
    public async Task Post_WithoutOid_RejectsBeforeTriggering()
    {
        var telemetry = new ApiTestTelemetry<RefreshController>();
        var controller = CreateController(ApiInterfaceProxy<IIngestionJobTrigger>.Create((_, _) =>
            throw new InvalidOperationException("Must not trigger")), telemetry, new Claim("sub", "subject-only"));

        Assert.Equal(403, (await controller.Post(Request(), default)).StatusCode);
        Assert.Equal("subject-only", Assert.Single(telemetry.Errors).CallerId);
    }

    [Theory]
    [InlineData("0")]
    [InlineData("-1")]
    [InlineData("CHW01")]
    [InlineData("2147483648")]
    [InlineData(" 42")]
    public async Task Post_InvalidDatabaseId_ReturnsBadRequest(string windfarmId)
    {
        var telemetry = new ApiTestTelemetry<RefreshController>();
        var controller = CreateController(ApiInterfaceProxy<IIngestionJobTrigger>.Create((_, _) =>
            throw new InvalidOperationException("Must not trigger")), telemetry, new Claim("oid", "caller"));

        Assert.Equal(400, (await controller.Post(Request() with { WindfarmId = windfarmId }, default)).StatusCode);
        Assert.IsType<ArgumentException>(Assert.Single(telemetry.Errors).Error);
    }

    [Fact]
    public async Task Post_MissingWindfarmName_ReturnsBadRequest()
    {
        var controller = CreateController(ApiInterfaceProxy<IIngestionJobTrigger>.Create((_, _) =>
            throw new InvalidOperationException("Must not trigger")), new(), new Claim("oid", "caller"));

        Assert.Equal(400, (await controller.Post(Request() with { WindfarmName = " " }, default)).StatusCode);
    }

    [Fact]
    public async Task Post_KubernetesConflict_ReturnsConflict()
    {
        var error = new HttpOperationException
        {
            Response = new HttpResponseMessageWrapper(new HttpResponseMessage(HttpStatusCode.Conflict), "conflict")
        };
        var telemetry = new ApiTestTelemetry<RefreshController>();
        var controller = CreateController(ApiInterfaceProxy<IIngestionJobTrigger>.Create((_, _) =>
            Task.FromException<string>(error)), telemetry, new Claim("oid", "caller"));

        Assert.Equal(409, (await controller.Post(Request(), default)).StatusCode);
        Assert.Same(error, Assert.Single(telemetry.Errors).Error);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public async Task Post_EmptyOrReversedScope_ReturnsBadRequestBeforeTriggering(int endOffsetDays)
    {
        var telemetry = new ApiTestTelemetry<RefreshController>();
        var controller = CreateController(ApiInterfaceProxy<IIngestionJobTrigger>.Create((_, _) =>
            throw new InvalidOperationException("Must not trigger")), telemetry, new Claim("oid", "caller"));
        var request = Request();

        var response = await controller.Post(request with { EndDateUtc = request.StartDateUtc.AddDays(endOffsetDays) }, default);

        Assert.Equal(400, response.StatusCode);
        Assert.IsType<ArgumentOutOfRangeException>(Assert.Single(telemetry.Errors).Error);
    }

    [Theory]
    [InlineData("")]
    [InlineData("\"StartDateUtc\":\"2026-09-01T00:00:00Z\",")]
    [InlineData("\"EndDateUtc\":\"2026-09-02T00:00:00Z\",")]
    [InlineData("\"StartDateUtc\":null,\"EndDateUtc\":\"2026-09-02T00:00:00Z\",")]
    [InlineData("\"StartDateUtc\":\"2026-09-01T00:00:00Z\",\"EndDateUtc\":null,")]
    [InlineData("\"StartDateUtc\":\"not-a-date\",\"EndDateUtc\":\"2026-09-02T00:00:00Z\",")]
    [InlineData("\"StartDateUtc\":\"2026-09-01T00:00:00Z\",\"EndDateUtc\":\"not-a-date\",")]
    public void RequestContract_RejectsMissingNullOrMalformedDatesDuringDeserialization(string dateProperties)
    {
        var json = "{" + dateProperties + "\"WindfarmId\":\"42\",\"WindfarmName\":\"CHW01\",\"Reason\":\"reconcile\"}";

        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<RefreshRequestDto>(json, new JsonSerializerOptions(JsonSerializerDefaults.Web)));
    }

    [Fact]
    public async Task Post_SubmissionFailure_IsLoggedAndRethrown()
    {
        var error = new InvalidOperationException("submission failed");
        var telemetry = new ApiTestTelemetry<RefreshController>();
        var controller = CreateController(ApiInterfaceProxy<IIngestionJobTrigger>.Create((_, _) =>
            Task.FromException<string>(error)), telemetry, new Claim("oid", "caller"));

        Assert.Same(error, await Assert.ThrowsAsync<InvalidOperationException>(() => controller.Post(Request(), default)));
        Assert.Same(error, Assert.Single(telemetry.Errors).Error);
    }

    [Fact]
    public async Task Post_ClientCancellation_PropagatesWithoutErrorLog()
    {
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        var telemetry = new ApiTestTelemetry<RefreshController>();
        var controller = CreateController(ApiInterfaceProxy<IIngestionJobTrigger>.Create((_, _) =>
            Task.FromCanceled<string>(cancellation.Token)), telemetry, new Claim("oid", "caller"));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => controller.Post(Request(), cancellation.Token));
        Assert.Empty(telemetry.Errors);
    }

    [Fact]
    public void RequestContract_HasNoClientOwnedAuditIdentity()
    {
        Assert.Equal(new[] { "EndDateUtc", "Reason", "StartDateUtc", "WindfarmId", "WindfarmName" },
            typeof(RefreshRequestDto).GetProperties().Select(property => property.Name).OrderBy(name => name).ToArray());
    }

    private static RefreshRequestDto Request() => new()
    {
        WindfarmId = "42", WindfarmName = "CHW01", Reason = "reconcile",
        StartDateUtc = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc),
        EndDateUtc = new DateTime(2026, 9, 2, 0, 0, 0, DateTimeKind.Utc)
    };

    private static RefreshController CreateController(IIngestionJobTrigger trigger,
        ApiTestTelemetry<RefreshController> telemetry, params Claim[] claims) =>
        new(new OutageRefreshPlanner(), trigger, telemetry)
        {
            ControllerContext = new ControllerContext
            {
                HttpContext = new DefaultHttpContext { User = new ClaimsPrincipal(new ClaimsIdentity(claims, "test")) }
            }
        };
}