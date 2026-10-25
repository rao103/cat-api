using System;

namespace CAT.RawFanOut.Models;

public sealed record RetentionCleanupSummary(
    DateTimeOffset CutoffUtc, int DeletedArtifacts, int DeletedBatches, TimeSpan Elapsed, int DeletedIncidents = 0);
