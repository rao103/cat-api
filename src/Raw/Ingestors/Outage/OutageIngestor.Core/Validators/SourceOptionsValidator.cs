using Microsoft.Extensions.Options;
using OutageIngestor.Configuration;
using OutageIngestor.Resolvers;

namespace OutageIngestor.Validators;

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
        ArgumentException.ThrowIfNullOrWhiteSpace(options.SourceSystemName);
        BlobConnectionSettingsResolver.Resolve(options);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.SqsQueueUrl);

        if (!Uri.TryCreate(options.SqsQueueUrl, UriKind.Absolute, out var queueUri)
            || (queueUri.Scheme != Uri.UriSchemeHttps && queueUri.Scheme != Uri.UriSchemeHttp))
            throw new ArgumentException("Source:SqsQueueUrl must be an absolute HTTP or HTTPS URL.");
    }
}
