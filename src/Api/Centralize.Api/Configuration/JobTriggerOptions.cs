namespace Centralize.Api.Configuration;

public sealed class JobTriggerOptions
{
    public const string SectionName = "JobTrigger";
    public string Namespace { get; set; } = "";
    public Dictionary<string, IngestorJobOptions> Sources { get; set; } = new(StringComparer.OrdinalIgnoreCase);
}

public sealed class IngestorJobOptions
{
    public string CronJobName { get; set; } = "";
    public string WorkerContainerName { get; set; } = "";
}
