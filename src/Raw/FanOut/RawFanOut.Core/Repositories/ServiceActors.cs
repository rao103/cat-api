namespace CAT.RawFanOut.Repositories;

// Stable configured service identities stamped into inserted_by/updated_by audit columns.
// These are application actors, not database logins or pod names; keep pod identity in telemetry.
internal static class ServiceActors
{
    public const string FanOutWorker = "raw-fanout-worker";
    public const string DeadLetterWorker = "raw-fanout-dlq-worker";

    public static string Operator(string name)
    {
        if (string.IsNullOrWhiteSpace(name)) throw new System.ArgumentException("Operator name is required.", nameof(name));
        return "operator:" + name.Trim();
    }
}
