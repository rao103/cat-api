using System.Globalization;
using System.Text.RegularExpressions;
using Centralize.Api.Configuration;
using Centralize.Api.Interfaces;
using Centralize.Core.Models;
using Centralize.Core.Validators;
using k8s;
using k8s.Models;
using Microsoft.Extensions.Options;

namespace Centralize.Api.Triggering;

public sealed class KubernetesIngestionJobTrigger(
    IKubernetes client, IOptions<JobTriggerOptions> options) : IIngestionJobTrigger
{
    public void ValidateSources(IReadOnlyList<RefreshSourceModel> sources)
    {
        if (string.IsNullOrWhiteSpace(options.Value.Namespace))
            throw new InvalidOperationException("JobTrigger Namespace is required.");
        foreach (var source in sources)
        {
            if (!options.Value.Sources.TryGetValue(source.SourceType, out var target) ||
                string.IsNullOrWhiteSpace(target.WorkerContainerName))
                throw new InvalidOperationException($"JobTrigger configuration is required for {source.SourceType}.");
            BuildJobName(target.CronJobName, new string('a', 32));
        }
    }

    public async Task<string> TriggerAsync(OperationRequestModel request, RefreshSourceModel source,
        Guid operationId, string runId, CancellationToken cancellationToken)
    {
        ValidateSources([source]);
        var target = options.Value.Sources[source.SourceType];
        var jobName = BuildJobName(target.CronJobName, runId);
        var cronJob = await client.BatchV1.ReadNamespacedCronJobAsync(target.CronJobName,
            options.Value.Namespace, cancellationToken: cancellationToken);
        var job = CreateRefreshJob(cronJob, request, source, operationId, runId, jobName, target.WorkerContainerName);
        await client.BatchV1.CreateNamespacedJobAsync(job, options.Value.Namespace, cancellationToken: cancellationToken);
        return jobName;
    }

    public static string BuildJobName(string cronJobName, string runId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(cronJobName);
        ArgumentException.ThrowIfNullOrWhiteSpace(runId);
        var name = $"{cronJobName}-refresh-{runId}";
        if (name.Length > 63 || !Regex.IsMatch(name, "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$"))
            throw new ArgumentException("CronJobName and RunId must form a lowercase Kubernetes Job name of at most 63 characters.");
        return name;
    }

    public static V1Job CreateRefreshJob(V1CronJob cronJob, OperationRequestModel request,
        RefreshSourceModel source, Guid operationId, string runId, string jobName, string workerContainerName)
    {
        request = OperationRequestValidator.Normalize(request);
        if (operationId == Guid.Empty) throw new ArgumentException("OperationId is required.");
        if (source.WindfarmId != request.WindfarmId || string.IsNullOrWhiteSpace(source.WindfarmName) ||
            !OperationRequestValidator.SupportedSources.Contains(source.SourceType) ||
            (request.SourceType is not null && request.SourceType != source.SourceType))
            throw new ArgumentException("The selected source does not match the refresh request.");
        ArgumentException.ThrowIfNullOrWhiteSpace(runId);
        var labels = new Dictionary<string, string>
        {
            ["triggered-by"] = "centralize-api", ["run-id"] = runId,
            ["operation-id"] = operationId.ToString("D")
        };
        var job = new V1Job
        {
            Metadata = new V1ObjectMeta { Name = jobName, Labels = labels },
            Spec = KubernetesJson.Deserialize<V1JobSpec>(KubernetesJson.Serialize(cronJob.Spec.JobTemplate.Spec))
        };
        job.Spec.Parallelism = 1;
        job.Spec.Completions = 1;
        job.Spec.CompletionMode = "NonIndexed";
        job.Spec.Template.Metadata ??= new V1ObjectMeta();
        job.Spec.Template.Metadata.Labels ??= new Dictionary<string, string>();
        foreach (var label in labels) job.Spec.Template.Metadata.Labels[label.Key] = label.Value;
        var container = job.Spec.Template.Spec.Containers.SingleOrDefault(item => item.Name == workerContainerName)
            ?? throw new InvalidOperationException("The CronJob does not contain the configured ingestor worker container.");
        if (container.Args?.Any(argument =>
            argument.Contains("REFRESH_", StringComparison.OrdinalIgnoreCase) ||
            argument.Contains("OPERATION_ID", StringComparison.OrdinalIgnoreCase) ||
            argument.Contains("Ingestion:WindfarmId", StringComparison.OrdinalIgnoreCase)) == true)
            throw new InvalidOperationException("The CronJob worker arguments conflict with refresh environment inputs.");
        container.Env = (container.Env ?? []).Where(item =>
            !item.Name.StartsWith("REFRESH_", StringComparison.Ordinal) && item.Name != "OPERATION_ID").ToList();
        container.Env.Add(new("OPERATION_ID", operationId.ToString("D")));
        container.Env.Add(new("REFRESH_WINDFARM_ID", source.WindfarmId.ToString(CultureInfo.InvariantCulture)));
        container.Env.Add(new("REFRESH_WINDFARM_NAME", source.WindfarmName));
        container.Env.Add(new("REFRESH_RUN_ID", runId));
        container.Env.Add(new("REFRESH_REQUESTED_BY", request.RequestedBy));
        container.Env.Add(new("REFRESH_REASON", request.Reason ?? ""));
        var start = request.StartDate.ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc).ToString("O", CultureInfo.InvariantCulture);
        var end = request.EndDate.AddDays(1).ToDateTime(TimeOnly.MinValue, DateTimeKind.Utc).ToString("O", CultureInfo.InvariantCulture);
        switch (source.SourceType)
        {
            case "Production":
                container.Env.Add(new("REFRESH_DATA_DATES", string.Join(',', OperationRequestValidator.GetDates(request)
                    .Select(date => date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)))));
                break;
            case "Outage":
                container.Env.Add(new("REFRESH_SCOPE_START_UTC", start));
                container.Env.Add(new("REFRESH_SCOPE_END_UTC", end));
                break;
            case "Alarm":
                container.Env.Add(new("REFRESH_MODE", "Historical"));
                container.Env.Add(new("REFRESH_HISTORICAL_START_UTC", start));
                container.Env.Add(new("REFRESH_HISTORICAL_END_UTC", end));
                break;
            // Curtailment refresh uses the source's event window, without date inputs.
        }
        return job;
    }
}
