using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using ProductionIngestor.Configuration;
using ProductionIngestor.Resolvers;

namespace ProductionIngestor.Validators;

/// <summary>Validates that SqlSourceOptions resolves cleanly against the legacy Source:ConnectionString fallback.</summary>
public sealed class SqlSourceOptionsValidator(IConfiguration configuration) : IValidateOptions<SqlSourceOptions>
{
    public ValidateOptionsResult Validate(string? name, SqlSourceOptions options)
    {
        try
        {
            SqlSourceSettingsResolver.Resolve(options, configuration["Source:ConnectionString"]);
            return ValidateOptionsResult.Success;
        }
        catch (ArgumentException exception)
        {
            return ValidateOptionsResult.Fail(exception.Message);
        }
    }
}
