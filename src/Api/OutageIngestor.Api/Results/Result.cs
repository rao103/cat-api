namespace OutageIngestor.Api.Results;

/// <summary>A default provided class that is being used to organize the actual result.</summary>
public class Result
{
    public Result(object obj)
    {
        Object = obj;
    }

    public Result(Exception exception)
    {
        Exception = exception;

        Error = $"{exception?.GetType().FullName}: {exception?.Message}.";

        if (!string.IsNullOrEmpty(exception?.StackTrace))
        {
            Error = $"{Error}. Stack: {exception?.StackTrace}.";
        }
    }

    public bool Success => Exception is null;

    public string? Error { get; }

    private Exception? Exception { get; }

    public object? Object { get; }

    public DateTime CreatedDateTimeUtc { get; } = DateTime.UtcNow;
}
