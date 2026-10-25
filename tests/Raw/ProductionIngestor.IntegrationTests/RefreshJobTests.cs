using k8s.Models;
using ProductionIngestor.Api.Triggering;
using ProductionIngestor.Models;

namespace ProductionIngestor.IntegrationTests;

public sealed class RefreshJobTests
{
    [Fact]
    public void MissingHistoryNeverMeansSuccess()
    {
        Assert.Equal("Unknown", RefreshStatusService.ResolveStatus(null, []));
        Assert.Equal("Unverified", RefreshStatusService.ResolveStatus(new V1Job { Status = new V1JobStatus { Succeeded = 1 } }, []));
        Assert.Equal("StartupFailed", RefreshStatusService.ResolveStatus(new V1Job
        {
            Status = new V1JobStatus { Conditions = [new V1JobCondition { Type = "Failed", Status = "True" }] }
        }, []));
    }

    [Theory]
    [InlineData("UPPER")]
    [InlineData("invalid/id")]
    [InlineData("")]
    public void InvalidJobIdentityIsRejectedBeforeSubmission(string requestId)
        => Assert.Throws<ArgumentException>(() => KubernetesIngestionJobTrigger.BuildJobName("production-ingestor", requestId));

    [Fact]
    public void RefreshClonesTemplateAndOverridesScheduledParallelism()
    {
        var cronJob = new V1CronJob
        {
            Spec = new V1CronJobSpec
            {
                Schedule = "0 2 * * *",
                JobTemplate = new V1JobTemplateSpec
                {
                    Spec = new V1JobSpec
                    {
                        Parallelism = 3,
                        Completions = 3,
                        Template = new V1PodTemplateSpec
                        {
                            Spec = new V1PodSpec
                            {
                                Containers = new List<V1Container>
                                {
                                    new() { Name = "worker", Image = "test:local", Env = new List<V1EnvVar>
                                    {
                                        new("Ingestion__JobName", "job-name"),
                                        new("REFRESH_WINDFARM_ID", "old")
                                    } },
                                    new() { Name = "sidecar", Image = "test:sidecar" }
                                }
                            }
                        }
                    }
                }
            }
        };
        var request = new RefreshRequestModel("42", "farm-42", new DateOnly(2026, 9, 1), new DateOnly(2026, 9, 1), "operator", "test", "run");
        var job = KubernetesIngestionJobTrigger.CreateRefreshJob(cronJob, request, "refresh-run", "worker");
        Assert.Equal(1, job.Spec.Parallelism);
        Assert.Equal(1, job.Spec.Completions);
        Assert.Equal("NonIndexed", job.Spec.CompletionMode);
        Assert.Equal(3, cronJob.Spec.JobTemplate.Spec.Parallelism);
        var environment = Assert.Single(job.Spec.Template.Spec.Containers.Where(container => container.Name == "worker")).Env;
        Assert.Null(job.Spec.Template.Spec.Containers.Single(container => container.Name == "sidecar").Env);
        Assert.Equal("42", Assert.Single(environment.Where(value => value.Name == "REFRESH_WINDFARM_ID")).Value);
        Assert.Equal("2026-09-01", Assert.Single(environment.Where(value => value.Name == "REFRESH_DATA_DATES")).Value);
        Assert.Equal("old", cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Env[1].Value);
    }

    [Fact]
    public void RefreshDateRangeIncludesBothEndpoints()
    {
        var request = new RefreshRequestModel("42", "farm-42", new DateOnly(2026, 9, 1),
            new DateOnly(2026, 9, 3), "operator", "test", "run");

        Assert.Equal(new[] { new DateOnly(2026, 9, 1), new DateOnly(2026, 9, 2), new DateOnly(2026, 9, 3) },
            request.GetDataDates());
    }

    [Fact]
    public void RefreshDateRangeRejectsEndBeforeStart()
    {
        var request = new RefreshRequestModel("42", "farm-42", new DateOnly(2026, 9, 3),
            new DateOnly(2026, 9, 1), "operator", "test", "run");

        Assert.Throws<ArgumentException>(() => request.GetDataDates());
    }
}