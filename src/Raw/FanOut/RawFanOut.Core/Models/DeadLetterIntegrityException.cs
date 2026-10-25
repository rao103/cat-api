using System;

namespace CAT.RawFanOut.Models;

// The same queue message identity was archived again with different content.
public sealed class DeadLetterIntegrityException(string queueUrl, string messageId)
    : InvalidOperationException($"Dead-letter message {messageId} on {queueUrl} was archived with different content.");
