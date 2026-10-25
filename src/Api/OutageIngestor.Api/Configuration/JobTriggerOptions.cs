namespace OutageIngestor.Api.Configuration;

/// <summary>Identifies the CronJob whose template is used to launch a one-shot refresh Job.</summary>
public sealed class JobTriggerOptions
{
    public const string SectionName = "JobTrigger";

    public required string Namespace { get; set; }

    public required string CronJobName { get; set; }

    public string WorkerContainerName { get; set; } = "outage-ingestor-worker";
}
