using Microsoft.Extensions.Options;
using AlarmIngestor.Configuration;

namespace AlarmIngestor.Validators;

public sealed class SourceOptionsValidator : IValidateOptions<SourceOptions>
{
    public ValidateOptionsResult Validate(string? name, SourceOptions options)
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

    public static void ValidateAndThrow(SourceOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(options.ApiBaseUri);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.SourceSystemName);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.BlobConnectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.BlobContainerName);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.ControlPlaneConnectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.SqsQueueUrl);

        if (!Uri.TryCreate(options.ApiBaseUri, UriKind.Absolute, out var apiUri)
            || (apiUri.Scheme != Uri.UriSchemeHttps && apiUri.Scheme != Uri.UriSchemeHttp))
            throw new ArgumentException("Source:ApiBaseUri must be an absolute HTTP or HTTPS URL.");

        if (!Uri.TryCreate(options.SqsQueueUrl, UriKind.Absolute, out var queueUri)
            || (queueUri.Scheme != Uri.UriSchemeHttps && queueUri.Scheme != Uri.UriSchemeHttp))
            throw new ArgumentException("Source:SqsQueueUrl must be an absolute HTTP or HTTPS URL.");
    }
}
