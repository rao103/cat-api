using System.Globalization;
using System.Net;
using System.Text.Json;
using k8s;
using k8s.Models;
using OutageIngestor.Api.Triggering;
using OutageIngestor.Models;
using OutageIngestor.Worker.Configuration;

namespace OutageIngestor.UnitTests;

public sealed class ApiJobTriggerTests
{
    [Fact]
    public void CreateRefreshJob_ClonesTemplate_TargetsOnlyWorker_ReplacesEnvironment_AndForcesOneShot()
    {
        var cronJob = Template();
        var before = KubernetesJson.Serialize(cronJob);
        var request = Request();
        var jobName = KubernetesIngestionJobTrigger.BuildJobName("outage-ingestor", request.RunId);

        var job = KubernetesIngestionJobTrigger.CreateRefreshJob(cronJob, request, jobName);

        Assert.Equal(before, KubernetesJson.Serialize(cronJob));
        Assert.Equal(1, job.Spec.Parallelism);
        Assert.Equal(1, job.Spec.Completions);
        Assert.Equal("NonIndexed", job.Spec.CompletionMode);
        Assert.Equal("outage-ingestor-api", job.Metadata.Labels["triggered-by"]);
        Assert.Equal(request.RunId, job.Metadata.Labels["run-id"]);
        var worker = job.Spec.Template.Spec.Containers.Single(container => container.Name == "outage-ingestor-worker");
        var env = worker.Env.ToDictionary(variable => variable.Name, variable => variable.Value);
        Assert.Equal("42", env["REFRESH_WINDFARM_ID"]);
        Assert.Equal("CHW01", env["REFRESH_WINDFARM_NAME"]);
        Assert.Equal(request.RunId, env["REFRESH_RUN_ID"]);
        Assert.Equal("caller-oid", env["REFRESH_REQUESTED_BY"]);
        Assert.Equal("reconcile", env["REFRESH_REASON"]);
        Assert.Equal("preserved", env["UNRELATED"]);
        Assert.Equal(request.ScopeStartUtc.ToString("O", CultureInfo.InvariantCulture), env["REFRESH_SCOPE_START_UTC"]);
        Assert.Equal(request.ScopeEndUtc.ToString("O", CultureInfo.InvariantCulture), env["REFRESH_SCOPE_END_UTC"]);
        var parsed = IngestionRequestParser.TryParse(new(null, env["REFRESH_WINDFARM_ID"], env["REFRESH_WINDFARM_NAME"],
            env["REFRESH_RUN_ID"], env["REFRESH_SCOPE_START_UTC"], env["REFRESH_SCOPE_END_UTC"]));
        Assert.True(parsed.Succeeded, parsed.Error);
        Assert.Equal(request.ScopeStartUtc, parsed.Request!.RefreshScopeStartUtc);
        Assert.Equal(request.ScopeEndUtc, parsed.Request.RefreshScopeEndUtc);
        Assert.Equal(DateTimeKind.Utc, parsed.Request.RefreshScopeStartUtc!.Value.Kind);
        Assert.Equal(DateTimeKind.Utc, parsed.Request.RefreshScopeEndUtc!.Value.Kind);
        var sidecar = job.Spec.Template.Spec.Containers.Single(container => container.Name == "sidecar");
        Assert.Equal("untouched", Assert.Single(sidecar.Env).Value);
    }

    [Fact]
    public void CreateRefreshJob_UsesConfiguredWorkerContainer()
    {
        var template = Template();
        template.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Name = "custom-worker";
        var job = KubernetesIngestionJobTrigger.CreateRefreshJob(template, Request(), "refresh-job", "custom-worker");
        Assert.Contains(job.Spec.Template.Spec.Containers[0].Env, variable => variable.Name == "REFRESH_WINDFARM_NAME");
    }

    [Theory]
    [InlineData("--REFRESH_WINDFARM_ID=9")]
    [InlineData("--Ingestion:WindfarmId=9")]
    [InlineData("--REFRESH_SCOPE_START_UTC=2025-01-01T00:00:00Z")]
    [InlineData("--REFRESH_SCOPE_END_UTC=2025-02-01T00:00:00Z")]
    public void CreateRefreshJob_RejectsConflictingWorkerArguments(string argument)
    {
        var template = Template();
        template.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Args = [argument];
        Assert.Throws<InvalidOperationException>(() => KubernetesIngestionJobTrigger.CreateRefreshJob(template, Request(), "refresh-job"));
    }

    [Fact]
    public void CreateRefreshJob_RejectsMissingWorker() =>
        Assert.Throws<InvalidOperationException>(() => KubernetesIngestionJobTrigger.CreateRefreshJob(Template(), Request(), "refresh-job", "missing"));

    [Theory]
    [InlineData("")]
    [InlineData("UPPERCASE")]
    [InlineData("bad/name")]
    [InlineData("ends-with-")]
    [InlineData("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")]
    public void BuildJobName_RejectsInvalidRunIds(string runId) =>
        Assert.ThrowsAny<ArgumentException>(() => KubernetesIngestionJobTrigger.BuildJobName("outage-ingestor", runId));

    [Fact]
    public async Task LocalTrigger_SendsExactScopeNameAndAudit_ParsedByWorker()
    {
        var request = Request();
        using var handler = new ApiHttpHandler(async (message, token) =>
        {
            Assert.Equal(HttpMethod.Post, message.Method);
            Assert.Equal("http://localhost:5188/debug/refresh/", message.RequestUri!.AbsoluteUri);
            var json = JsonSerializer.Deserialize<JsonElement>(await message.Content!.ReadAsStringAsync(token));
            Assert.Equal("42", json.GetProperty("windfarmId").GetString());
            Assert.Equal("CHW01", json.GetProperty("windfarmName").GetString());
            Assert.Equal("caller-oid", json.GetProperty("requestedBy").GetString());
            Assert.Equal("reconcile", json.GetProperty("reason").GetString());
            Assert.Equal(request.RunId, json.GetProperty("runId").GetString());
            Assert.Equal(request.ScopeStartUtc.ToString("O", CultureInfo.InvariantCulture), json.GetProperty("scopeStartUtc").GetString());
            Assert.Equal(request.ScopeEndUtc.ToString("O", CultureInfo.InvariantCulture), json.GetProperty("scopeEndUtc").GetString());
            var parsed = IngestionRequestParser.TryParse(new(null, json.GetProperty("windfarmId").GetString(),
                json.GetProperty("windfarmName").GetString(), json.GetProperty("runId").GetString(),
                json.GetProperty("scopeStartUtc").GetString(), json.GetProperty("scopeEndUtc").GetString()));
            Assert.True(parsed.Succeeded, parsed.Error);
            Assert.Equal(request.ScopeStartUtc, parsed.Request!.RefreshScopeStartUtc);
            Assert.Equal(request.ScopeEndUtc, parsed.Request.RefreshScopeEndUtc);
            Assert.False(json.TryGetProperty("dataDates", out _));
            Assert.False(json.TryGetProperty("startDate", out _));
            return new HttpResponseMessage(HttpStatusCode.Accepted);
        });
        using var client = new HttpClient(handler) { BaseAddress = new Uri("http://localhost:5188/") };

        Assert.Equal($"local-refresh-{request.RunId}", await new LocalIngestionJobTrigger(client).TriggerAsync(request, default));
    }

    [Fact]
    public async Task LocalTrigger_DoesNotReportAcceptanceAfterHttpFailure()
    {
        using var handler = new ApiHttpHandler((_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)));
        using var client = new HttpClient(handler) { BaseAddress = new Uri("http://localhost:5188/") };
        await Assert.ThrowsAsync<HttpRequestException>(() => new LocalIngestionJobTrigger(client).TriggerAsync(Request(), default));
    }

    [Fact]
    public async Task LocalTrigger_PropagatesCancellation()
    {
        using var cancellation = new CancellationTokenSource();
        using var handler = new ApiHttpHandler((_, token) =>
        {
            cancellation.Cancel();
            token.ThrowIfCancellationRequested();
            throw new InvalidOperationException("Cancellation was not forwarded");
        });
        using var client = new HttpClient(handler) { BaseAddress = new Uri("http://localhost:5188/") };
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            new LocalIngestionJobTrigger(client).TriggerAsync(Request(), cancellation.Token));
    }

    private static RefreshRequestModel Request() => new(42, "CHW01", "caller-oid", "reconcile", "0123456789abcdef0123456789abcdef",
        new DateTime(2026, 9, 1, 3, 4, 5, DateTimeKind.Utc).AddTicks(1234567),
        new DateTime(2026, 9, 2, 6, 7, 8, DateTimeKind.Utc).AddTicks(7654321));

    private static V1CronJob Template() => new()
    {
        Spec = new V1CronJobSpec
        {
            Schedule = "* * * * *",
            JobTemplate = new V1JobTemplateSpec
            {
                Spec = new V1JobSpec
                {
                    Parallelism = 4, Completions = 4, CompletionMode = "Indexed",
                    Template = new V1PodTemplateSpec
                    {
                        Spec = new V1PodSpec
                        {
                            Containers =
                            [
                                new V1Container
                                {
                                    Name = "outage-ingestor-worker", Image = "outage:test",
                                    Env =
                                    [
                                        new("REFRESH_WINDFARM_ID", "old"), new("REFRESH_WINDFARM_NAME", "old"),
                                        new("REFRESH_RUN_ID", "old"), new("REFRESH_REQUESTED_BY", "old"),
                                        new("REFRESH_SCOPE_START_UTC", "2025-01-01T00:00:00Z"),
                                        new("REFRESH_SCOPE_END_UTC", "2025-02-01T00:00:00Z"),
                                        new("REFRESH_REASON", "old"), new("UNRELATED", "preserved")
                                    ]
                                },
                                new V1Container { Name = "sidecar", Image = "sidecar:test", Env = [new("REFRESH_RUN_ID", "untouched")] }
                            ]
                        }
                    }
                }
            }
        }
    };
}