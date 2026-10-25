using System.Globalization;

namespace ProductionIngestor.EndToEndTests;

/// <summary>
/// Explicit opt-in gate for the live Worker end-to-end test. Reads only environment variables;
/// touches no external resource. Absence of opt-in must resolve to "skip", never "pass" or "fail".
/// </summary>
public static class LiveTestGate
{
    public const string OptInVariable = "PRODUCTION_E2E_LIVE";
    public const string WindfarmIdVariable = "PRODUCTION_E2E_WINDFARM_ID";
    public const string DateVariable = "PRODUCTION_E2E_DATE";
    public const string SqsObservationConfirmedVariable = "PRODUCTION_E2E_SQS_OBSERVATION_CONFIRMED";
    public const string TimeoutMinutesVariable = "PRODUCTION_E2E_TIMEOUT_MINUTES";

    public static bool IsOptedIn()
        => string.Equals(Environment.GetEnvironmentVariable(OptInVariable), "true", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Parses and validates every mandatory live-test input. Throws <see cref="ArgumentException"/>
    /// with a specific message for the first missing/malformed prerequisite; never contacts a
    /// provider.
    /// </summary>
    public static LiveTestRequest ParseAndValidate()
    {
        var windfarmIdText = Environment.GetEnvironmentVariable(WindfarmIdVariable);
        if (string.IsNullOrWhiteSpace(windfarmIdText))
            throw new ArgumentException($"{WindfarmIdVariable} must be set to a positive integer.");
        if (!int.TryParse(windfarmIdText, NumberStyles.None, CultureInfo.InvariantCulture, out var windfarmId) || windfarmId <= 0)
            throw new ArgumentException($"{WindfarmIdVariable} must be a positive integer.");

        var dateText = Environment.GetEnvironmentVariable(DateVariable);
        if (string.IsNullOrWhiteSpace(dateText))
            throw new ArgumentException($"{DateVariable} must be set to a single yyyy-MM-dd value.");
        if (!DateOnly.TryParseExact(dateText, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var date))
            throw new ArgumentException($"{DateVariable} must be a single yyyy-MM-dd value.");

        if (!string.Equals(Environment.GetEnvironmentVariable(SqsObservationConfirmedVariable), "true", StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException(
                $"{SqsObservationConfirmedVariable} must be 'true': an operator must confirm the configured dev SQS " +
                "queue has no competing consumers for the observation window before this test can prove message receipt.");

        var timeoutMinutes = 10;
        var timeoutText = Environment.GetEnvironmentVariable(TimeoutMinutesVariable);
        if (!string.IsNullOrWhiteSpace(timeoutText))
        {
            if (!int.TryParse(timeoutText, NumberStyles.None, CultureInfo.InvariantCulture, out timeoutMinutes) || timeoutMinutes <= 0)
                throw new ArgumentException($"{TimeoutMinutesVariable} must be a positive integer number of minutes.");
        }

        return new LiveTestRequest(windfarmId.ToString(CultureInfo.InvariantCulture), date, TimeSpan.FromMinutes(timeoutMinutes));
    }
}

public sealed record LiveTestRequest(string WindfarmId, DateOnly ProductionDate, TimeSpan Timeout);
