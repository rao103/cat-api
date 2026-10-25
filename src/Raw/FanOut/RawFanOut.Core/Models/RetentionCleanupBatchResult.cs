namespace CAT.RawFanOut.Models;

public sealed record RetentionCleanupBatchResult(int DeletedArtifacts, int DeletedBatches);