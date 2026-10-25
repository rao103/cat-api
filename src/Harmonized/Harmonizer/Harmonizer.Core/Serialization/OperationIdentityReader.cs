using System.Text.Json;
using Harmonizer.Core.Models;

namespace Harmonizer.Core.Serialization;

/// <summary>Reads additive correlation fields without changing supplied V2/V3 wire contracts.</summary>
public static class OperationIdentityReader
{
    public static OperationIdentity Read(JsonElement work, string originalMessageBody)
    {
        var current = ReadFields(work);
        using var original = JsonDocument.Parse(originalMessageBody, new() { MaxDepth = 32 });
        if (original.RootElement.ValueKind != JsonValueKind.Object)
        {
            throw new WorkValidationException("OriginalMessageBody must contain a JSON envelope.");
        }

        var source = ReadFields(original.RootElement);
        return new(Merge(current.OperationId, source.OperationId, "OperationId"),
            Merge(current.ExecutionId, source.ExecutionId, "ExecutionId"));
    }

    private static OperationIdentity ReadFields(JsonElement envelope) =>
        new(ReadField(envelope, "OperationId"), ReadField(envelope, "ExecutionId"));

    private static string? ReadField(JsonElement envelope, string name)
    {
        var fields = envelope.EnumerateObject().Where(property => property.NameEquals(name)).ToArray();
        if (fields.Length > 1)
        {
            throw new WorkValidationException($"Duplicate {name} field in an operation envelope.");
        }

        if (fields.Length == 0 || fields[0].Value.ValueKind == JsonValueKind.Null)
        {
            return null;
        }

        if (fields[0].Value.ValueKind != JsonValueKind.String)
        {
            throw new WorkValidationException($"{name} must be a string identifier.");
        }

        var value = fields[0].Value.GetString();
        if (string.IsNullOrWhiteSpace(value) || value.Length > 256 || value.Any(char.IsControl))
        {
            throw new WorkValidationException($"{name} must be a nonblank identifier of at most 256 characters without control characters.");
        }

        return value;
    }

    private static string? Merge(string? workValue, string? sourceValue, string name)
    {
        if (workValue is not null && sourceValue is not null && workValue != sourceValue)
        {
            throw new WorkValidationException($"{name} disagrees with the original source envelope.");
        }

        return workValue ?? sourceValue;
    }
}
