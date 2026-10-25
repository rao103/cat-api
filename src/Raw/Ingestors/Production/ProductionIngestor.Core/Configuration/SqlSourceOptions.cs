namespace ProductionIngestor.Configuration;

public sealed class SqlSourceOptions
{
    public AzureSourceOptions Azure { get; set; } = new();
    public List<SourceConnectionStringOptions> ConnectionStrings { get; set; } = [];
    public List<SourceServicePrincipalOptions> ServicePrincipals { get; set; } = [];
}

public sealed class AzureSourceOptions
{
    public string TenantId { get; set; } = string.Empty;
}

public sealed class SourceConnectionStringOptions
{
    public string Key { get; set; } = string.Empty;
    public bool Enabled { get; set; }
    public string Value { get; set; } = string.Empty;
    public string ServicePrincipalKey { get; set; } = string.Empty;
}

public sealed class SourceServicePrincipalOptions
{
    public string Key { get; set; } = string.Empty;
    public bool Enabled { get; set; }
    public string ClientId { get; set; } = string.Empty;
    public string Secret { get; set; } = string.Empty;
    public string Resource { get; set; } = string.Empty;
    public int ExpirationThresholdInMinutes { get; set; } = 60;
}