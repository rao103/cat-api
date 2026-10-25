using System;
#nullable enable
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using CAT.RawFanOut.Observability;
using CAT.RawFanOut.Models;
using CAT.RawFanOut.Operations.DeadLetters;
using CAT.RawFanOut.Operations.Retention;
using CAT.RawFanOut.Worker.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace CAT.RawFanOut.Worker.Hosting;

public static class WorkerExecution
{
    public static async Task RunAsync(HostApplicationBuilder builder, WorkerOptions options, string[] args)
    {
        using var host = builder.Build();
        var replay = Array.IndexOf(args, "--replay");
        var cleanup = Array.IndexOf(args, "--cleanup");
        var resolve = Array.IndexOf(args, "--resolve");
        if (resolve >= 0)
        {
            var id = ReadGuid(args, resolve);
            var reason = ReadOption(args, "--reason");
            var operatorName = ReadOption(args, "--operator");
            using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(options.Processing.MaxProcessingSeconds));
            await host.StartAsync(cancellation.Token);
            try
            {
                await host.Services.GetRequiredService<ResolveIncident>()
                    .ExecuteAsync(new ResolveIncidentRequest(id, reason, operatorName), cancellation.Token);
            }
            catch (Exception)
            {
                Environment.ExitCode = 1;
                throw;
            }
            finally
            {
                using var shutdown = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                await host.StopAsync(shutdown.Token);
            }
        }
        else if (cleanup >= 0)
        {
            using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(options.Processing.MaxProcessingSeconds));
            Console.CancelKeyPress += (_, e) => { e.Cancel = true; cancellation.Cancel(); };
            await host.StartAsync(cancellation.Token);
            try
            {
                await host.Services.GetRequiredService<RetentionCleanupJob>().ExecuteAsync(cancellation.Token);
            }
            catch (Exception)
            {
                Environment.ExitCode = 1;
                throw;
            }
            finally
            {
                using var shutdown = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                await host.StopAsync(shutdown.Token);
            }
        }
        else if (replay >= 0)
        {
            if (replay + 1 >= args.Length || !Guid.TryParse(args[replay + 1], out var id))
                throw new ArgumentException("Usage: --replay <incident-uuid>");
            using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(options.Processing.MaxProcessingSeconds));
            Console.CancelKeyPress += (_, e) => { e.Cancel = true; cancellation.Cancel(); };
            await host.StartAsync(cancellation.Token);
            try
            {
                var replayLogger = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("CAT.RawFanOut.Replay");
                using var activity = FanOutDiagnostics.Activities.StartActivity("artifact.replay");
                using var scope = replayLogger.BeginScope(new Dictionary<string, object?> { ["IncidentId"] = id });
                try
                {
                    replayLogger.LogInformation("Starting incident replay");
                    await host.Services.GetRequiredService<ReplayIncident>().ExecuteAsync(id, cancellation.Token);
                    replayLogger.LogInformation("Replay dispatch completed; incident stays unresolved and downstream harmonization remains asynchronous");
                }
                catch (Exception error)
                {
                    activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error);
                    replayLogger.LogError(error, "Replay failed. ErrorType={ErrorType}", error.GetType().FullName);
                    Environment.ExitCode = 1;
                }
            }
            finally
            {
                using var shutdown = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                await host.StopAsync(shutdown.Token);
            }
        }
        else
        {
            await host.RunAsync();
        }
    }

    private static Guid ReadGuid(string[] args, int flagIndex)
    {
        if (flagIndex + 1 >= args.Length || !Guid.TryParse(args[flagIndex + 1], out var id))
            throw new ArgumentException("Usage: --resolve <incident-uuid> --reason <text> --operator <name>");
        return id;
    }

    private static string ReadOption(string[] args, string name)
    {
        var index = Array.IndexOf(args, name);
        if (index < 0 || index + 1 >= args.Length || string.IsNullOrWhiteSpace(args[index + 1]))
            throw new ArgumentException($"{name} is required. Usage: --resolve <incident-uuid> --reason <text> --operator <name>");
        return args[index + 1];
    }
}