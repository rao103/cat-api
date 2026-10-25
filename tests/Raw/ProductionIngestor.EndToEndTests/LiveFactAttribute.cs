namespace ProductionIngestor.EndToEndTests;

/// <summary>
/// A <see cref="FactAttribute"/> that reports as an actual xUnit "Skipped" result (not a silent
/// pass) when <see cref="LiveTestGate.OptInVariable"/> is not "true", instead of relying on an
/// early-return inside the test body.
/// </summary>
public sealed class LiveFactAttribute : FactAttribute
{
    public LiveFactAttribute()
    {
        if (!LiveTestGate.IsOptedIn())
            Skip = $"Live end-to-end test skipped: {LiveTestGate.OptInVariable} is not 'true'.";
    }
}
