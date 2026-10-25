using Microsoft.Extensions.Options;
using CurtailmentIngestor.Configuration;

namespace CurtailmentIngestor.Validators;

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
        if (options.SafetyLagInterval < TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(options.SafetyLagInterval));
        if (options.MaxCatchUpWindowSize <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(options.MaxCatchUpWindowSize));
        if (options.LeaseDuration < TimeSpan.FromMilliseconds(3) || options.LeaseDuration > TimeSpan.FromDays(1))
            throw new ArgumentOutOfRangeException(nameof(options.LeaseDuration), "Lease duration must be between 3 milliseconds and one day.");
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.MaxEventsPerShard);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.MaxEventsPerWindow);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.MaxSnapshotEvents);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.ParquetRowGroupSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.NormalCadenceConcurrencyLimit);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.BackfillConcurrencyLimit);
    }
}
