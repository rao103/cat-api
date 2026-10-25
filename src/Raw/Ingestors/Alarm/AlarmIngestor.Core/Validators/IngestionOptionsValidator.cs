using Microsoft.Extensions.Options;
using AlarmIngestor.Configuration;

namespace AlarmIngestor.Validators;

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
        if (options.LeaseDuration < TimeSpan.FromMilliseconds(3) || options.LeaseDuration > TimeSpan.FromDays(1))
            throw new ArgumentOutOfRangeException(nameof(options.LeaseDuration), "Lease duration must be between 3 milliseconds and one day.");
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.SourceQueryPageSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.PollBatchSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.MaxEventsPerShard);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.MaxOpenAlarmsPerTurbine);
        if (options.RequestTimeout <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(options.RequestTimeout));

        if (options.Windfarms.Count == 0)
            throw new ArgumentException("Ingestion:Windfarms must configure at least one windfarm.");

        foreach (var windfarm in options.Windfarms)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(windfarm.WindfarmId);
            if (windfarm.AlarmCodes.Count == 0)
                throw new ArgumentException($"Ingestion:Windfarms[{windfarm.WindfarmId}].AlarmCodes must not be empty.");
            if (windfarm.Turbines.Count == 0)
                throw new ArgumentException($"Ingestion:Windfarms[{windfarm.WindfarmId}].Turbines must not be empty.");
            foreach (var turbine in windfarm.Turbines)
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(turbine.TurbineRdspp);
                ArgumentException.ThrowIfNullOrWhiteSpace(turbine.ItemId);
            }
        }
    }
}
