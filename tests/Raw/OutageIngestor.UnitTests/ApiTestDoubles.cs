using System.Reflection;
using Microsoft.Extensions.FileProviders;
using Microsoft.Extensions.Hosting;
using OutageIngestor.Api.Interfaces;

namespace OutageIngestor.UnitTests;

internal sealed class ApiTestTelemetry<T> : ITelemetry<T>
{
    public List<(string Message, string? RunId, string? JobName, string? CallerId)> InformationEvents { get; } = [];
    public List<(Exception Error, string? RunId, string? CallerId)> Errors { get; } = [];

    public void Information(string message, string? runId = null, string? jobName = null, string? callerId = null) =>
        InformationEvents.Add((message, runId, jobName, callerId));

    public void Error(string message, Exception error, string? runId = null, string? callerId = null) =>
        Errors.Add((error, runId, callerId));
}

// No provider calls: fail every interface member unless a test explicitly handles it.
public class ApiInterfaceProxy<T> : DispatchProxy where T : class
{
    public Func<MethodInfo, object?[], object?> Handler { get; set; } = (_, _) => throw new NotSupportedException();

    public static T Create(Func<MethodInfo, object?[], object?> handler)
    {
        var instance = DispatchProxy.Create<T, ApiInterfaceProxy<T>>();
        ((ApiInterfaceProxy<T>)(object)instance).Handler = handler;
        return instance;
    }

    protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) =>
        Handler(targetMethod!, args ?? []);
}

internal sealed class ApiHttpHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> send) : HttpMessageHandler
{
    protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
        send(request, cancellationToken);
}

internal sealed class ApiHostEnvironment : IHostEnvironment
{
    public string EnvironmentName { get; set; } = Environments.Development;
    public string ApplicationName { get; set; } = "OutageIngestor.Api";
    public string ContentRootPath { get; set; } = AppContext.BaseDirectory;
    public IFileProvider ContentRootFileProvider { get; set; } = new NullFileProvider();
}