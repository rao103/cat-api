using System.Net;
using System.Text.Json;
using System.Threading.Channels;

namespace ProductionIngestor.Worker.Operations;

/// <summary>Loopback-only development bridge; executes one refresh at a time.</summary>
public sealed class LocalRefreshListener(
    IServiceScopeFactory scopeFactory,
    ILogger<LocalRefreshListener> logger) : BackgroundService
{
    private readonly Channel<LocalRefreshCommand> _requests = Channel.CreateBounded<LocalRefreshCommand>(
        new BoundedChannelOptions(8) { SingleReader = true, SingleWriter = false, FullMode = BoundedChannelFullMode.Wait });

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var listener = new HttpListener();
        listener.Prefixes.Add("http://localhost:5187/debug/refresh/");
        listener.Start();
        logger.LogInformation("Local refresh listener ready on localhost:5187.");

        var consumer = ConsumeAsync(stoppingToken);
        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var context = await listener.GetContextAsync().WaitAsync(stoppingToken);
                await HandleAsync(context, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
        finally
        {
            listener.Stop();
            _requests.Writer.TryComplete();
            await consumer;
        }
    }

    private async Task HandleAsync(HttpListenerContext context, CancellationToken cancellationToken)
    {
        try
        {
            if (context.Request.HttpMethod != "POST")
            {
                context.Response.StatusCode = 405;
                return;
            }

            var command = await JsonSerializer.DeserializeAsync<LocalRefreshCommand>(
                context.Request.InputStream,
                new JsonSerializerOptions(JsonSerializerDefaults.Web), cancellationToken);

            if (command is null || string.IsNullOrWhiteSpace(command.WindfarmId)
                || string.IsNullOrWhiteSpace(command.RunId)
                || string.IsNullOrWhiteSpace(command.RequestedBy)
                || string.IsNullOrWhiteSpace(command.DataDates))
            {
                context.Response.StatusCode = 400;
                return;
            }

            // A successful response means queued locally, not that ingestion succeeded.
            await _requests.Writer.WriteAsync(command, cancellationToken);
            context.Response.StatusCode = 202;
        }
        catch (JsonException)
        {
            context.Response.StatusCode = 400;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            context.Response.StatusCode = 503;
        }
        finally
        {
            context.Response.Close();
        }
    }

    private async Task ConsumeAsync(CancellationToken stoppingToken)
    {
        try
        {
            await foreach (var command in _requests.Reader.ReadAllAsync(stoppingToken))
            {
                try
                {
                    using var scope = scopeFactory.CreateScope();
                    var execution = scope.ServiceProvider.GetRequiredService<IngestionExecutionService>();
                    var result = await execution.RunRefreshAsync(
                        command.WindfarmId, command.WindfarmName, command.DataDates,
                        command.RunId, command.RequestedBy, command.Reason, stoppingToken, command.OperationId);
                    logger.LogInformation("Local refresh {RunId} exited with code {ExitCode}.", command.RunId, result.ExitCode);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { break; }
                catch (Exception error)
                {
                    logger.LogError(error, "Local refresh {RunId} failed.", command.RunId);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested) { }
    }

    private sealed record LocalRefreshCommand(
        string WindfarmId, string WindfarmName, string RunId,
        string RequestedBy, string? Reason, string DataDates)
    {
        public string? OperationId { get; init; }
    }
}
