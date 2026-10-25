using System.ComponentModel.DataAnnotations;

namespace Harmonizer.Core.Configuration;

public sealed class EventLifecycleOptions
{
    public bool Enabled { get; init; } = true;
    [Range(1, 300)] public int PollSeconds { get; init; } = 30;
    [Range(1, 8)] public int ConcurrentFarms { get; init; } = 2;
    [Range(1, 100)] public int FarmsPerPoll { get; init; } = 20;
    [Range(1, 366)] public int DaysPerPass { get; init; } = 31;
    [Range(1, 10000)] public int MaximumPartitions { get; init; } = 1200;
    [Range(-30, 0)] public int ProcessableDayOffset { get; init; } = -1;
    [Range(1, 3600)] public int RetrySeconds { get; init; } = 30;
}