using System;

namespace CAT.RawFanOut.Models;

public sealed record WorkSelection(string WorkKind, DateOnly? DataDate, int[] ItemIds, string[] EventIds)
{
    public string[] RawPaths { get; init; } = [];
    public int[] ScopeItemIds { get; init; } = [];
    public bool HasNoSourceRows { get; init; }
}