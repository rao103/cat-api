using CAT.RawFanOut.Configuration;
using System;
using System.Linq;
namespace CAT.RawFanOut.Worker.Configuration;
public sealed class WorkerOptions
{
    public string Region { get; set; } = "eu-north-1";
    public string RawQueueUrl { get; set; } = "";
    public string RawDlqUrl { get; set; } = "";
    public string HarmonizationQueueUrl { get; set; } = "";
    public string RawContainerUri { get; set; } = "";
    public string RawPathPrefixToStrip { get; set; } = "";
    public int ConsumerCount { get; set; } = 1;
    public bool EnableDeadLetterConsumer { get; set; } = true;
    public FanOutOptions Processing { get; set; } = new();
    public RetentionCleanupOptions Retention { get; set; } = new();
    public void Validate()
    {
        Processing.Validate();
        Retention.Validate();
        if (string.IsNullOrWhiteSpace(Region) || ConsumerCount is < 1 or > 16)
            throw new ArgumentException("Invalid region or consumer count.");
        foreach (var queue in new[] { RawQueueUrl, RawDlqUrl, HarmonizationQueueUrl })
            if (!Uri.TryCreate(queue, UriKind.Absolute, out var uri) || uri.Scheme != "https" || queue.EndsWith(".fifo"))
                throw new ArgumentException("Configure three Standard SQS HTTPS queue URLs.");
        if (new[] { RawQueueUrl, RawDlqUrl, HarmonizationQueueUrl }.Distinct(StringComparer.Ordinal).Count() != 3)
            throw new ArgumentException("Input, output, and DLQ must be different queues.");
        if (!Uri.TryCreate(RawContainerUri, UriKind.Absolute, out var blob) || blob.Scheme != "https" ||
            !string.IsNullOrEmpty(blob.Query)) throw new ArgumentException("Configure an HTTPS Blob container URI without SAS.");
    }
}
