namespace CAT.RawFanOut.Models;

// ErrorType/LastError are set only from evidence the worker actually has (e.g. an envelope validation failure).
public sealed record DeadLetterArchiveRequest(
    string QueueUrl, QueueDelivery Delivery, RawEnvelope? Artifact, string? ErrorType = null, string? LastError = null);
