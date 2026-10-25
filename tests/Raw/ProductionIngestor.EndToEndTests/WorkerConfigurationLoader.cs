using Microsoft.Extensions.Configuration;

namespace ProductionIngestor.EndToEndTests;

/// <summary>
/// Builds an <see cref="IConfiguration"/> identical in shape to the one the Worker process itself
/// loads (same content root, same base/environment appsettings, same user-secrets id, same
/// environment-variable overrides), so verification clients resolve the same external endpoints
/// the launched Worker will use. Never writes to any of these sources.
/// </summary>
public static class WorkerConfigurationLoader
{
    /// <summary>Directory of ProductionIngestor.Worker's project file (its content root at run time).</summary>
    public static string ResolveWorkerContentRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "ProductionIngestor.Worker.csproj")))
        {
            directory = directory.Parent;
        }

        if (directory is not null)
            return directory.FullName;

        // Fallback for a solution-relative layout when the test assembly is not nested under the Worker's own bin/obj.
        var repoRoot = FindRepoRoot();
        var contentRoot = Path.Combine(repoRoot, "src", "Raw", "Ingestors", "Production", "ProductionIngestor.Worker");
        if (!Directory.Exists(contentRoot))
            throw new InvalidOperationException("Could not resolve ProductionIngestor.Worker's content root.");
        return contentRoot;
    }

    private static string FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "O.WP.CAT.LossCalculation.Platform.sln")))
        {
            directory = directory.Parent;
        }
        return directory?.FullName
            ?? throw new InvalidOperationException("Could not resolve the repository root from the test assembly location.");
    }

    public static IConfiguration Load()
    {
        var contentRoot = ResolveWorkerContentRoot();

        // Host.CreateApplicationBuilder (used by Worker's Program.cs) reads only DOTNET_ENVIRONMENT
        // and defaults to "Production" when unset -- unlike ASP.NET Core hosts, it does not
        // recognize ASPNETCORE_ENVIRONMENT and does not default to "Development".
        var environmentName = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? "Production";

        var builder = new ConfigurationBuilder()
            .SetBasePath(contentRoot)
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: false)
            .AddJsonFile($"appsettings.{environmentName}.json", optional: true, reloadOnChange: false);

        // Matches the Worker's own UserSecretsId (ProductionIngestor.Worker.csproj). The generic
        // host only auto-adds the user-secrets provider when the environment is "Development";
        // mirror that here so this loader can't see local secrets a Production-mode Worker run
        // would never have loaded.
        if (string.Equals(environmentName, "Development", StringComparison.OrdinalIgnoreCase))
            builder.AddUserSecrets("dotnet-ProductionIngestor-f8ffd430-5ed5-49bd-ada0-7716541f3661");
        builder.AddEnvironmentVariables();

        return builder.Build();
    }
}
