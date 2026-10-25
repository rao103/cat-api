namespace Harmonizer.Core.Models;

public sealed class WorkValidationException(string message) : Exception(message);
public sealed class UnsupportedWorkException(string message) : Exception(message);
public sealed class SourceConflictException(string message) : Exception(message);
public sealed class LeaseUnavailableException(string message) : Exception(message);
public sealed class LeaseLostException(string message) : Exception(message);
