using System.ComponentModel.DataAnnotations;

namespace Harmonizer.Core.Configuration;

public sealed class HarmonizerOptions
{
    public const string Section = "Harmonizer";
    public EventLifecycleOptions EventLifecycle { get; init; } = new();
    [Range(1, 50)] public int MaximumTurbinesPerBatch { get; init; } = 50;
    [Range(1, 10)] public int ConcurrentMessages { get; init; } = 2;
    [Range(60, 3600)] public int LeaseSeconds { get; init; } = 180;
    [Range(10, 120)] public int HeartbeatSeconds { get; init; } = 30;
    [Range(60, 43200)] public int VisibilitySeconds { get; init; } = 180;
    [Range(1, 20)] public int PollSeconds { get; init; } = 20;
    [Range(1, 10000)] public int MaximumRawPaths { get; init; } = 1000;
    [Range(144, 5000000)] public int MaximumRowsPerArtifact { get; init; } = 1000000;
    [Range(1048576, 2147483647)] public long MaximumArtifactBytes { get; init; } = 536870912;
    [Required] public string QueueUrl { get; init; } = "";
    [Required] public string BlobContainerUrl { get; init; } = "";
    [Required] public string OutputBlobContainerUrl { get; init; } = "";
    [Required] public string PostgresConnectionString { get; init; } = "";
    public bool PublishNotifications { get; init; }
    public bool UseRdsIamAuthentication { get; init; }
    public string? NotificationQueueUrl { get; init; }
    public string? BlobConnectionString { get; init; }
    public string? OutputBlobConnectionString { get; init; }
    public string? SqsServiceUrl { get; init; }
    public string AwsRegion { get; init; } = "eu-north-1";
}
