using System.Collections.Generic;

namespace CAT.RawFanOut.Models;

public sealed record FanOutPlanningContext(RawEnvelope Artifact, FarmCatalog Farm, IReadOnlyList<RawIdentity> Rows);