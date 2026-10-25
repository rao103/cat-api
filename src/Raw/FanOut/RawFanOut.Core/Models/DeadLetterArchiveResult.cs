using System;

namespace CAT.RawFanOut.Models;

public sealed record DeadLetterArchiveResult(Guid IncidentId, bool Created);
