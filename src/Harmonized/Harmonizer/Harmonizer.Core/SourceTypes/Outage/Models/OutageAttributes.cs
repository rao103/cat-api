using System.Collections.ObjectModel;
using System.Text.Json;

namespace Harmonizer.Core.SourceTypes.Outage.Models;

/// <summary>Preserves descriptive source fields without making them part of interval processing.</summary>
public sealed class OutageAttributes
{
    public IReadOnlyDictionary<string, JsonElement> Values { get; }

    public OutageAttributes(IReadOnlyDictionary<string, JsonElement> values)
    {
        // Clone values so their lifetime does not depend on the source JsonDocument or dictionary.
        var copy = values.ToDictionary(pair => pair.Key, pair => pair.Value.Clone(), StringComparer.Ordinal);
        Values = new ReadOnlyDictionary<string, JsonElement>(copy);
    }

    public bool ContentEquals(OutageAttributes other) =>
        Values.Count == other.Values.Count && Values.All(pair =>
            other.Values.TryGetValue(pair.Key, out var value) && JsonElement.DeepEquals(pair.Value, value));
}
