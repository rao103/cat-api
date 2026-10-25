using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
namespace CAT.RawFanOut.Configuration;
public sealed class FanOutOptions
{
    public int ItemBatchSize { get; set; } = 50;
    public int EventBatchSize { get; set; } = 50;
    public int MaxMessageBytes { get; set; } = 240_000;
    public int MaxBatchBytes { get; set; } = 240_000;
    public long MaxPlanBytes { get; set; } = 64 * 1024 * 1024;
    public int MaxShards { get; set; } = 128;
    public int MaxIdentities { get; set; } = 100_000;
    public int MaxScopeDatesPerExtraction { get; set; } = 36_600;
    public int MaxFilesPerExtraction { get; set; } = 100_000;
    public long MaxRowsPerArtifact { get; set; } = 10_000_000;
    public long MaxRowsPerRowGroup { get; set; } = 250_000;
    public long MaxFileBytes { get; set; } = 1_073_741_824;
    public int VisibilitySeconds { get; set; } = 300;
    public int HeartbeatSeconds { get; set; } = 60;
    public int MaxProcessingSeconds { get; set; } = 3600;
    public int PublishAttempts { get; set; } = 3;
    public void Validate()
    {
        if (ItemBatchSize < 1 || EventBatchSize < 1 || MaxShards < 1 || MaxIdentities < 1 || MaxScopeDatesPerExtraction < 1 || MaxFilesPerExtraction < 1 ||
            MaxPlanBytes < 1024 || MaxRowsPerArtifact < 1 || MaxRowsPerRowGroup < 1 || MaxFileBytes < 1 ||
            MaxMessageBytes < 1024 || MaxMessageBytes > 1_048_576 ||
            MaxBatchBytes < MaxMessageBytes || MaxBatchBytes > 1_048_576 ||
            VisibilitySeconds < 30 || VisibilitySeconds > 43200 || HeartbeatSeconds < 1 ||
            HeartbeatSeconds * 3 > VisibilitySeconds || MaxProcessingSeconds < VisibilitySeconds ||
            MaxProcessingSeconds > 39600 || PublishAttempts is < 1 or > 10)
            throw new ArgumentException("Invalid FanOut limits, visibility, or retry configuration.");
    }
}
