using k8s;
using System.Globalization;
using System.Net;
using k8s.Autorest;
using k8s.Models;
using Microsoft.Extensions.Options;
using OutageIngestor.Api.Configuration;
using OutageIngestor.Api.Interfaces;
using OutageIngestor.Models;

namespace OutageIngestor.Api.Triggering;

/// <summary>
/// Clones the configured CronJob's pod template into a one-shot Job carrying the refresh request,
/// so the actual ingestion pass still runs in the existing OutageIngestor batch image/process.
/// </summary>
public sealed class KubernetesIngestionJobTrigger : IIngestionJobTrigger
{
    private readonly IKubernetes _client;
    private readonly JobTriggerOptions _options;
    private readonly ITelemetry<KubernetesIngestionJobTrigger> _telemetry;

    public KubernetesIngestionJobTrigger(IKubernetes client, IOptions<JobTriggerOptions> options,
        ITelemetry<KubernetesIngestionJobTrigger> telemetry)
    {
        _client = client;
        _options = options.Value;
        _telemetry = telemetry;
    }

    public async Task<string> TriggerAsync(RefreshRequestModel request, CancellationToken cancellationToken)
    {
        var jobName = BuildJobName(_options.CronJobName, request.RunId);
        try
        {
            var cronJob = await _client.BatchV1.ReadNamespacedCronJobAsync(_options.CronJobName, _options.Namespace, cancellationToken: cancellationToken);
            var job = CreateRefreshJob(cronJob, request, jobName, _options.WorkerContainerName);
            await _client.BatchV1.CreateNamespacedJobAsync(job, _options.Namespace, cancellationToken: cancellationToken);
            _telemetry.Information("Kubernetes refresh job created.", request.RunId, jobName);
            return jobName;
        }
        catch (Exception error) when (error is not ArgumentException
            && (error is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
            && error is not HttpOperationException { Response.StatusCode: HttpStatusCode.Conflict })
        {
            _telemetry.Error("Kubernetes refresh job creation failed.", error, request.RunId);
            throw;
        }
    }

    public static string BuildJobName(string cronJobName, string requestId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(requestId);
        var jobName = $"{cronJobName}-refresh-{requestId}";
        if (jobName.Length > 63 || !System.Text.RegularExpressions.Regex.IsMatch(jobName, "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"))
            throw new ArgumentException("RunId must form a lowercase Kubernetes Job name of at most 63 characters including the CronJob prefix.", nameof(requestId));
        return jobName;
    }

    public static V1Job CreateRefreshJob(V1CronJob cronJob, RefreshRequestModel request, string jobName,
        string workerContainerName = "outage-ingestor-worker")
    {
        var job = new V1Job
        {
            Metadata = new V1ObjectMeta
            {
                Name = jobName,
                Labels = new Dictionary<string, string> { ["triggered-by"] = "outage-ingestor-api", ["run-id"] = request.RunId }
            },
            Spec = KubernetesJson.Deserialize<V1JobSpec>(KubernetesJson.Serialize(cronJob.Spec.JobTemplate.Spec))
        };

        job.Spec.Parallelism = 1;
        job.Spec.Completions = 1;
        job.Spec.CompletionMode = "NonIndexed";

        var container = job.Spec.Template.Spec.Containers.SingleOrDefault(candidate => candidate.Name == workerContainerName)
            ?? throw new InvalidOperationException("The CronJob does not contain the configured ingestor worker container.");
        if (container.Args?.Any(argument => argument.Contains("REFRESH_", StringComparison.OrdinalIgnoreCase)
            || argument.Contains("Ingestion:WindfarmId", StringComparison.OrdinalIgnoreCase)) == true)
            throw new InvalidOperationException("The CronJob worker arguments conflict with refresh environment inputs.");

        container.Env ??= [];
        var refreshNames = new HashSet<string>(StringComparer.Ordinal)
        {
            "REFRESH_WINDFARM_ID", "REFRESH_WINDFARM_NAME", "REFRESH_RUN_ID", "REFRESH_REQUESTED_BY", "REFRESH_REASON",
            "REFRESH_SCOPE_START_UTC", "REFRESH_SCOPE_END_UTC"
        };
        container.Env = container.Env.Where(variable => !refreshNames.Contains(variable.Name)).ToList();
        container.Env.Add(new V1EnvVar("REFRESH_WINDFARM_ID", request.WindfarmId.ToString(CultureInfo.InvariantCulture)));
        container.Env.Add(new V1EnvVar("REFRESH_WINDFARM_NAME", request.WindfarmName));
        container.Env.Add(new V1EnvVar("REFRESH_RUN_ID", request.RunId));
        container.Env.Add(new V1EnvVar("REFRESH_REQUESTED_BY", request.RequestedBy));
        container.Env.Add(new V1EnvVar("REFRESH_REASON", request.Reason));
        container.Env.Add(new V1EnvVar("REFRESH_SCOPE_START_UTC", request.ScopeStartUtc.ToString("O", CultureInfo.InvariantCulture)));
        container.Env.Add(new V1EnvVar("REFRESH_SCOPE_END_UTC", request.ScopeEndUtc.ToString("O", CultureInfo.InvariantCulture)));
        return job;
    }
}
