using Microsoft.Extensions.Options;
using OutageIngestor.Configuration;

namespace OutageIngestor.Validators;

public sealed class IngestionOptionsValidator : IValidateOptions<IngestionOptions>
{
    public ValidateOptionsResult Validate(string? name, IngestionOptions options)
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

    public static void ValidateAndThrow(IngestionOptions options)
    {
        if (options.MinimumIngestionInterval <= TimeSpan.Zero || options.FailureRetryBackoff <= TimeSpan.Zero
            || options.MaxInvocationDuration <= TimeSpan.Zero || options.MaxInvocationDuration > TimeSpan.FromDays(1))
            throw new ArgumentException("Ingestion interval, retry backoff and invocation duration must be positive; invocation duration must not exceed one day.");
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.MaxClaimsPerInvocation);

        if (options.SafetyLagInterval < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(options.SafetyLagInterval));
        if (options.MaxCatchUpWindowSize <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(options.MaxCatchUpWindowSize));
        if (options.LeaseDuration < TimeSpan.FromMilliseconds(3) || options.LeaseDuration > TimeSpan.FromDays(1))
            throw new ArgumentOutOfRangeException(nameof(options.LeaseDuration), "Lease duration must be between 3 milliseconds and one day.");
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.MaxEventsPerShard);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.NormalCadenceConcurrencyLimit);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.BackfillConcurrencyLimit);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.SourceQueryBatchSize);
    }
}
