using System.Diagnostics;
using System.Text;

namespace ProductionIngestor.EndToEndTests;

/// <summary>
/// Launches the already-built Worker executable as a child process for one bounded refresh run.
/// Requires the build output to exist; never restores or builds it. Enforces the overall timeout
/// with process-tree termination so a hung Worker cannot outlive the test.
/// </summary>
public static class WorkerProcessRunner
{
    public static string ResolveWorkerExecutablePath(string workerContentRoot)
    {
        var configuration =
#if DEBUG
            "Debug";
#else
            "Release";
#endif
        var binRoot = Path.Combine(workerContentRoot, "bin", configuration, "net10.0");
        var executableName = OperatingSystem.IsWindows() ? "ProductionIngestor.Worker.exe" : "ProductionIngestor.Worker";
        var executablePath = Path.Combine(binRoot, executableName);
        if (!File.Exists(executablePath))
            throw new FileNotFoundException(
                $"Worker build output was not found at '{executablePath}'. Build ProductionIngestor.Worker before running the live end-to-end test.",
                executablePath);
        return executablePath;
    }

    public static async Task<WorkerRunResult> RunRefreshAsync(
        string workerContentRoot, string windfarmId, DateOnly productionDate, string runId,
        IReadOnlyDictionary<string, string> environmentOverrides, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var executablePath = ResolveWorkerExecutablePath(workerContentRoot);

        using var timeoutSource = new CancellationTokenSource(timeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutSource.Token);

        // Guard against a cancellation that already fired (overall timeout/test cancellation)
        // before the process is even launched: starting a Worker we would immediately have to
        // kill is wasted work and could still leave it briefly alive if Start() races the check.
        linked.Token.ThrowIfCancellationRequested();

        var startInfo = new ProcessStartInfo
        {
            FileName = executablePath,
            WorkingDirectory = workerContentRoot,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };

        foreach (var (key, value) in environmentOverrides)
            startInfo.Environment[key] = value;

        // Overrides applied last: refresh identity for this test run must win over anything
        // inherited from the ambient environment.
        startInfo.Environment["REFRESH_WINDFARM_ID"] = windfarmId;
        startInfo.Environment["REFRESH_DATA_DATES"] = productionDate.ToString("yyyy-MM-dd");
        startInfo.Environment["REFRESH_RUN_ID"] = runId;

        using var process = new Process { StartInfo = startInfo };
        var stdout = new StringBuilder();
        var stderr = new StringBuilder();
        process.OutputDataReceived += (_, e) => { if (e.Data is not null) stdout.AppendLine(e.Data); };
        process.ErrorDataReceived += (_, e) => { if (e.Data is not null) stderr.AppendLine(e.Data); };

        process.Start();
        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        var timedOut = false;
        var killed = false;
        try
        {
            await process.WaitForExitAsync(linked.Token);
        }
        catch (OperationCanceledException)
        {
            // Any cancellation source (overall/outer or the local timeout) must stop the Worker;
            // only the timeoutSource distinguishes "timed out" for reporting purposes, but either
            // source leaves the process without a real exit code to report.
            killed = true;
            timedOut = timeoutSource.IsCancellationRequested;
            TryKillProcessTree(process);
            // Bound the wait for the OS to actually finish tearing the tree down; do not block indefinitely.
            using var killGrace = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            try { await process.WaitForExitAsync(killGrace.Token); } catch (OperationCanceledException) { }
        }

        return new WorkerRunResult(killed ? null : process.ExitCode, timedOut, stdout.ToString(), stderr.ToString());
    }

    private static void TryKillProcessTree(Process process)
    {
        try
        {
            if (!process.HasExited)
                process.Kill(entireProcessTree: true);
        }
        catch (InvalidOperationException)
        {
            // Process already exited between the check and the kill call.
        }
    }
}

public sealed record WorkerRunResult(int? ExitCode, bool TimedOut, string StandardOutput, string StandardError);
