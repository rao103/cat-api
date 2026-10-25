using Microsoft.Extensions.Options;
using ProductionIngestor.Configuration;
using ProductionIngestor.Resolvers;

namespace ProductionIngestor.Validators;

/// <summary>Validates SourceOptions against its sibling options; constructed per-host where the dependencies are registered.</summary>
public sealed class SourceOptionsValidator(
    IOptions<SqlSourceOptions> sqlSourceOptions,
    IOptions<SqsOptions> sqsOptions,
    IOptions<ControlPlaneOptions> controlPlaneOptions) : IValidateOptions<SourceOptions>
{
    public ValidateOptionsResult Validate(string? name, SourceOptions options)
    {
        try
        {
            ValidateAndThrow(options, sqlSourceOptions.Value, sqsOptions.Value, controlPlaneOptions.Value);
            return ValidateOptionsResult.Success;
        }
        catch (ArgumentException exception)
        {
            return ValidateOptionsResult.Fail(exception.Message);
        }
    }

    public static void ValidateAndThrow(SourceOptions options)
        => ValidateAndThrow(options, new SqlSourceOptions());

    public static void ValidateAndThrow(SourceOptions options, SqlSourceOptions sqlSourceOptions)
    {
        ValidateSourceSettings(options, sqlSourceOptions);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.SqsQueueUrl);
        if (!Uri.TryCreate(options.SqsQueueUrl, UriKind.Absolute, out var queueUri)
            || (queueUri.Scheme != Uri.UriSchemeHttps && queueUri.Scheme != Uri.UriSchemeHttp))
            throw new ArgumentException("SqsQueueUrl must be an absolute HTTP or HTTPS URL.");
    }

    public static void ValidateAndThrow(SourceOptions options, SqlSourceOptions sqlSourceOptions, SqsOptions sqsOptions)
    {
        ValidateSourceSettings(options, sqlSourceOptions);
        SqsOptionsValidator.ValidateAndThrow(sqsOptions);
    }

    public static void ValidateAndThrow(SourceOptions options, SqlSourceOptions sqlSourceOptions, SqsOptions sqsOptions,
        ControlPlaneOptions controlPlaneOptions)
    {
        ValidateSourceSettings(options, sqlSourceOptions, controlPlaneOptions);
        SqsOptionsValidator.ValidateAndThrow(sqsOptions);
    }

    private static void ValidateSourceSettings(SourceOptions options, SqlSourceOptions sqlSourceOptions,
        ControlPlaneOptions? controlPlaneOptions = null)
    {
        SqlSourceSettingsResolver.Resolve(sqlSourceOptions, options.ConnectionString);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.SourceSystemName);
        BlobConnectionSettingsResolver.Resolve(options);
        if (controlPlaneOptions is null)
            ArgumentException.ThrowIfNullOrWhiteSpace(options.ControlPlaneConnectionString);
        else
            ControlPlaneOptionsValidator.ValidateAndThrow(controlPlaneOptions);
    }
}
