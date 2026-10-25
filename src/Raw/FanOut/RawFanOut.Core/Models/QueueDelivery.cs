using System.Collections.Generic;

namespace CAT.RawFanOut.Models;

public sealed record QueueDelivery(string MessageId, string ReceiptHandle, string Body,
    IReadOnlyDictionary<string, string> Attributes)
{
    public string? TraceParent { get; init; }
    public string? TraceState { get; init; }
}