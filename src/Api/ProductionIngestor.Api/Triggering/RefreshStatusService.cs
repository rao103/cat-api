using System.Net;
using k8s;
using k8s.Autorest;
using k8s.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using ProductionIngestor.Api.Configuration;
using ProductionIngestor.Interfaces;
using ProductionIngestor.Models;

namespace ProductionIngestor.Api.Triggering;

public sealed record RefreshStatusResponse(
    string RunId,
    string JobName,
    string Status,
    IReadOnlyList<ExtractionRunHistoryModel> Attempts,
    bool EndToEndCompletionVerified = false);

public sealed class RefreshStatusService(
    IKubernetes client,
    IOptions<JobTriggerOptions> options,
    IExtractionRunHistoryRepository history,
    IHostEnvironment environment,
    IConfiguration configuration)
{
    public async Task<RefreshStatusResponse> GetAsync(
        ExtractionHistoryQueryModel query,
        CancellationToken cancellationToken)
    {
        var attempts = await history.GetAttemptsAsync(query, cancellationToken);

        if (environment.IsDevelopment() &&
            configuration.GetValue<bool>("LocalRefresh:Enabled"))
        {
            return new(
                query.RequestId,
                $"local-refresh-{query.RequestId}",
                ResolveStatus(null, attempts),
                attempts);
        }

        var jobName = KubernetesIngestionJobTrigger.BuildJobName(
            options.Value.CronJobName, query.RequestId);

        V1Job? job;
        try
        {
            job = await client.BatchV1.ReadNamespacedJobAsync(
                jobName,
                options.Value.Namespace,
                cancellationToken: cancellationToken);
        }
        catch (HttpOperationException error)
            when (error.Response.StatusCode == HttpStatusCode.NotFound)
        {
            job = null;
        }

        return new(
            query.RequestId,
            jobName,
            ResolveStatus(job, attempts),
            attempts);
    }

    public static string ResolveStatus(
        V1Job? job,
        IReadOnlyList<ExtractionRunHistoryModel> attempts)
    {
        var latest = attempts.FirstOrDefault();

        if (latest?.Status == "Completed")
            return "IngestionSucceeded";

        if (latest?.Status == "Running" &&
            latest.LeaseExpiresAt > DateTimeOffset.UtcNow)
            return "Running";

        if (job?.Status?.Conditions?.Any(condition =>
                condition.Type == "Failed" && condition.Status == "True") == true)
            return latest is null ? "StartupFailed" : "Failed";

        if (latest?.Status == "Running")
            return "LeaseExpired";

        if (latest is not null)
            return latest.Status;

        if (job?.Status?.Succeeded > 0)
            return "Unverified";

        if (job is null)
            return "Unknown";

        return job.Status?.Active > 0 ? "Starting" : "Accepted";
    }
}