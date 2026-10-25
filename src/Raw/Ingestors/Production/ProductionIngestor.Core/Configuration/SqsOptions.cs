namespace ProductionIngestor.Configuration;

public sealed class SqsOptions
{
    public const string SectionName = "Sqs";

    public string Region { get; set; } = string.Empty;
    public RawFanOutQueueOptions RawFanOut { get; set; } = new();
}

public sealed class RawFanOutQueueOptions
{
    public string QueueUrl { get; set; } = string.Empty;
}