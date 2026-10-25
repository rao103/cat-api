namespace Harmonizer.Core.Serialization;

/// <summary>Validates storage keys independently of any source payload.</summary>
public static class ObjectPath
{
    public static bool IsSafe(string path) =>
        !string.IsNullOrWhiteSpace(path) &&
        !path.StartsWith('/') &&
        !path.Contains('\\') &&
        !path.Contains("://", StringComparison.Ordinal) &&
        !path.Split('/').Any(segment => segment is ".." or "." or "");
}
