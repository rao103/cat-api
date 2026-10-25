namespace OutageIngestor.Enums;

public enum RunOutcome
{
    Success,
    LeaseHeldByOther,
    Zombie,
    Failed,
    Cancelled,
}
