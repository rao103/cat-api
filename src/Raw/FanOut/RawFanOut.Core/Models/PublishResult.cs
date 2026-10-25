using System.Collections.Generic;

namespace CAT.RawFanOut.Models;

public sealed record PublishResult(IReadOnlyList<string> Succeeded, IReadOnlyList<PublishFailure> Failed);