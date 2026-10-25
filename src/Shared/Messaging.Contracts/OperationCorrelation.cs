using System.Text.Json;

namespace CAT.Messaging.Contracts;

/// <summary>Operation IDs are opaque correlation strings; never substitute run or lease IDs.</summary>
public static class OperationCorrelation
{
    public static void Validate(string? value)
    {
        if (value is not null && (string.IsNullOrWhiteSpace(value) || value.Length > 256 || value.Any(char.IsControl)))
            throw new InvalidDataException("OperationId must be a nonblank string of at most 256 characters without control characters.");
    }

    public static string Require(string? value)
    {
        Validate(value);
        return value ?? throw new InvalidDataException("The extraction run has no persisted OperationId.");
    }

    public static string? Read(JsonElement envelope)
    {
        var fields = envelope.EnumerateObject().Where(p => p.NameEquals("OperationId")).ToArray();
        if (fields.Length > 1) throw new InvalidDataException("Duplicate OperationId.");
        if (fields.Length == 0 || fields[0].Value.ValueKind == JsonValueKind.Null) return null;
        if (fields[0].Value.ValueKind != JsonValueKind.String) throw new InvalidDataException("OperationId must be a string.");
        var value = fields[0].Value.GetString();
        Validate(value);
        return value;
    }
}
