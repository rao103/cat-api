using System;

namespace CAT.RawFanOut.Models;

public sealed record RawIdentity(int WindfarmId, int? WtgId, string? EventId, DateOnly? ProductionDate)
{
    // Only for legacy event/production artifacts that carry a turbine name instead of its numeric ID.
    public string? WtgName { get; init; }
    public string? RawFarmName { get; init; }
    public string? RawPath { get; init; }
}