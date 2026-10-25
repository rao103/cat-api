using Centralize.Api.Configuration;
using Centralize.Api.Triggering;
using Centralize.Core.Models;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Options;

namespace Centralize.UnitTests;

public sealed class KubernetesJobTriggerTests
{
    private static readonly Guid Operation = Guid.Parse("aaaaaaaa-1111-2222-3333-444444444444");
    private const string Run = "0123456789abcdef0123456789abcdef";

    [Theory]
    [InlineData("Production")]
    [InlineData("Outage")]
    [InlineData("Alarm")]
    [InlineData("Curtailment")]
    public void CreateRefreshJob_ClonesTemplateAndPreservesSourceContract(string source)
    {
        var template = Template(source);
        var before = KubernetesJson.Serialize(template);
        var job = KubernetesIngestionJobTrigger.CreateRefreshJob(template, Request(source),
            new(12, "WOW02", source), Operation, Run, "refresh-job", source.ToLowerInvariant() + "-ingestor-worker");
        Assert.Equal(before, KubernetesJson.Serialize(template));
        Assert.Equal(1, job.Spec.Parallelism);
        Assert.Equal(1, job.Spec.Completions);
        Assert.Equal("NonIndexed", job.Spec.CompletionMode);
        Assert.Equal("centralize-api", job.Metadata.Labels["triggered-by"]);
        Assert.Equal(Operation.ToString("D"), job.Spec.Template.Metadata.Labels["operation-id"]);
        Assert.Equal("worker-account", job.Spec.Template.Spec.ServiceAccountName);
        var worker = job.Spec.Template.Spec.Containers[0];
        var env = worker.Env.ToDictionary(item => item.Name, item => item.Value);
        Assert.Equal(Operation.ToString("D"), env["OPERATION_ID"]);
        Assert.Equal("12", env["REFRESH_WINDFARM_ID"]);
        Assert.Equal("WOW02", env["REFRESH_WINDFARM_NAME"]);
        Assert.Equal(Run, env["REFRESH_RUN_ID"]);
        Assert.Equal("caller", env["REFRESH_REQUESTED_BY"]);
        Assert.Equal("reconcile", env["REFRESH_REASON"]);
        Assert.Equal("preserved", env["UNRELATED"]);
        Assert.Equal("untouched", Assert.Single(job.Spec.Template.Spec.Containers[1].Env).Value);
        Assert.DoesNotContain("REFRESH_OLD", env.Keys);
        switch (source)
        {
            case "Production":
                Assert.Equal("2026-09-01,2026-09-02", env["REFRESH_DATA_DATES"]);
                Assert.DoesNotContain("REFRESH_SCOPE_START_UTC", env.Keys);
                break;
            case "Outage":
                Assert.Equal("2026-09-01T00:00:00.0000000Z", env["REFRESH_SCOPE_START_UTC"]);
                Assert.Equal("2026-09-03T00:00:00.0000000Z", env["REFRESH_SCOPE_END_UTC"]);
                Assert.DoesNotContain("REFRESH_DATA_DATES", env.Keys);
                break;
            case "Alarm":
                Assert.Equal("Historical", env["REFRESH_MODE"]);
                Assert.Equal("2026-09-01T00:00:00.0000000Z", env["REFRESH_HISTORICAL_START_UTC"]);
                Assert.Equal("2026-09-03T00:00:00.0000000Z", env["REFRESH_HISTORICAL_END_UTC"]);
                break;
            case "Curtailment":
                Assert.DoesNotContain("REFRESH_DATA_DATES", env.Keys);
                Assert.DoesNotContain("REFRESH_HISTORICAL_START_UTC", env.Keys);
                break;
        }
    }

    [Theory]
    [InlineData("--REFRESH_WINDFARM_ID=9")]
    [InlineData("--Ingestion:WindfarmId=9")]
    [InlineData("--OPERATION_ID=old")]
    public void CreateRefreshJob_RejectsConflictingArguments(string argument)
    {
        var template = Template("Production");
        template.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Args = [argument];
        Assert.Throws<InvalidOperationException>(() => KubernetesIngestionJobTrigger.CreateRefreshJob(
            template, Request("Production"), new(12, "WOW02", "Production"), Operation, Run, "refresh", "production-ingestor-worker"));
    }

    [Fact]
    public void CreateRefreshJob_RejectsMissingWorkerAndMismatchedIdentity()
    {
        Assert.Throws<InvalidOperationException>(() => KubernetesIngestionJobTrigger.CreateRefreshJob(
            Template("Production"), Request("Production"), new(12, "WOW02", "Production"),
            Operation, Run, "refresh", "missing"));
        Assert.Throws<ArgumentException>(() => KubernetesIngestionJobTrigger.CreateRefreshJob(
            Template("Production"), Request("Production"), new(13, "WOW02", "Production"),
            Operation, Run, "refresh", "production-ingestor-worker"));
        Assert.Throws<ArgumentException>(() => KubernetesIngestionJobTrigger.CreateRefreshJob(
            Template("Production"), Request("Production"), new(12, "WOW02", "Production"),
            Guid.Empty, Run, "refresh", "production-ingestor-worker"));
    }

    [Theory]
    [InlineData("bad/name", "abc")]
    [InlineData("production-ingestor", "UPPERCASE")]
    [InlineData("production-ingestor", "ends-with-")]
    [InlineData("", "abc")]
    [InlineData("aaaaaaaaaaaaaaaaaaaaaaaa", "0123456789abcdef0123456789abcdef")]
    public void JobName_RejectsInvalidOrLongNames(string cron, string run) =>
        Assert.ThrowsAny<ArgumentException>(() => KubernetesIngestionJobTrigger.BuildJobName(cron, run));

    [Fact]
    public void JobName_UsesExistingCronJobPrefix() =>
        Assert.Equal("production-ingestor-refresh-" + Run,
            KubernetesIngestionJobTrigger.BuildJobName("production-ingestor", Run));

    [Fact]
    public void ValidateSources_RejectsMissingSourceAndAdjustment()
    {
        using var client = new Kubernetes(new KubernetesClientConfiguration { Host = "http://127.0.0.1:1" });
        var trigger = new KubernetesIngestionJobTrigger(client, Options.Create(new JobTriggerOptions
        {
            Namespace = "unit",
            Sources = new() { ["Production"] = new() { CronJobName = "production-ingestor", WorkerContainerName = "production-ingestor-worker" } }
        }));
        trigger.ValidateSources([new(12, "WOW02", "Production")]);
        Assert.Throws<InvalidOperationException>(() => trigger.ValidateSources([new(12, "WOW02", "Outage")]));
        Assert.Throws<InvalidOperationException>(() => trigger.ValidateSources([new(12, "WOW02", "Adjustment")]));
    }

    private static OperationRequestModel Request(string source) =>
        new(12, new(2026, 9, 1), new(2026, 9, 2), "caller", source, "reconcile");

    private static V1CronJob Template(string source) => new()
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
                            ServiceAccountName = "worker-account",
                            Containers =
                            [
                                new V1Container
                                {
                                    Name = source.ToLowerInvariant() + "-ingestor-worker", Image = "worker:test",
                                    Env = [new("OPERATION_ID", "old"), new("REFRESH_OLD", "old"), new("UNRELATED", "preserved")]
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
