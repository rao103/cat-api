using OutageIngestor.Core.Constants;

namespace OutageIngestor.Configuration;

/// <summary>
/// Control-plane (extraction_cursor/extraction_run) connection and identity configuration.
/// Independent of ProductionIngestor's ControlPlaneOptions — each ingestor owns its own
/// configuration shape, per the repository's per-ingestor Core convention.
/// </summary>
public sealed class ControlPlaneOptions
{
    public const string SectionName = Constant.ControlPlane;

    public string Provider { get; set; } = Constant.PostgreSql;
    public string Authentication { get; set; } = Constant.AwsIam;
    public string ConnectionString { get; set; } = string.Empty;
    public string Host { get; set; } = string.Empty;
    public int Port { get; set; } = 5432;
    public string Database { get; set; } = string.Empty;
    public string Username { get; set; } = string.Empty;
    public string Region { get; set; } = string.Empty;
    public string AwsProfile { get; set; } = string.Empty;
    public string SigningHost { get; set; } = string.Empty;
    public int SigningPort { get; set; } = 5432;
    public string RootCertificate { get; set; } = string.Empty;
}
