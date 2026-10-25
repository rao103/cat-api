namespace Harmonizer.Core.Models;

/// <summary>Business operation and farm/source execution IDs are owned by the upstream trigger.</summary>
public sealed record OperationIdentity(string? OperationId = null, string? ExecutionId = null)
{
    public static OperationIdentity Untracked { get; } = new();
    public bool IsTracked => OperationId is not null || ExecutionId is not null;
}
