namespace CAT.RawFanOut.Models;

public sealed record PublishFailure(string Id, bool Permanent, string Code);