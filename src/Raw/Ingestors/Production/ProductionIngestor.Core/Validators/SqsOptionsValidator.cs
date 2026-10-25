using System.Text.RegularExpressions;
using Amazon;
using Microsoft.Extensions.Options;
using ProductionIngestor.Configuration;

namespace ProductionIngestor.Validators;

public sealed class SqsOptionsValidator : IValidateOptions<SqsOptions>
{
    public ValidateOptionsResult Validate(string? name, SqsOptions options)
    {
        try
        {
            ValidateAndThrow(options);
            return ValidateOptionsResult.Success;
        }
        catch (ArgumentException exception)
        {
            return ValidateOptionsResult.Fail(exception.Message);
        }
    }

    public static void ValidateAndThrow(SqsOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.Region)
            || !RegionEndpoint.EnumerableAllRegions.Any(endpoint => endpoint.SystemName == options.Region))
            throw new ArgumentException("Sqs:Region must identify a region supported by the installed AWS SDK.");

        var suffix = options.Region.StartsWith("cn-", StringComparison.Ordinal) ? "amazonaws.com.cn" : "amazonaws.com";
        var expectedHost = $"sqs.{options.Region}.{suffix}";
        if (options.RawFanOut is null
            || !Uri.TryCreate(options.RawFanOut.QueueUrl, UriKind.Absolute, out var queueUri)
            || queueUri.Scheme != Uri.UriSchemeHttps
            || !string.Equals(queueUri.Host, expectedHost, StringComparison.OrdinalIgnoreCase)
            || !queueUri.IsDefaultPort
            || !string.IsNullOrEmpty(queueUri.UserInfo)
            || !string.IsNullOrEmpty(queueUri.Query)
            || !string.IsNullOrEmpty(queueUri.Fragment)
            || !Regex.IsMatch(queueUri.AbsolutePath, "\\A/[0-9]{12}/[A-Za-z0-9_-]{1,80}\\z", RegexOptions.CultureInvariant)
            || !string.Equals(options.RawFanOut.QueueUrl, queueUri.AbsoluteUri, StringComparison.Ordinal))
            throw new ArgumentException("Sqs:RawFanOut:QueueUrl must be a canonical HTTPS standard SQS queue URL matching Sqs:Region, with a 12-digit account ID and queue name.");
    }
}
