namespace OutageIngestor.Api.Configuration;

/// <summary>AAD JWT bearer configuration for API authentication and OpenAPI's OAuth2 flow.</summary>
public sealed class AzureAdOptions
{
    public const string SectionName = "AzureAd";

    public required string Instance { get; set; }

    public required string TenantId { get; set; }

    public required string ClientId { get; set; }

    public string[] Scopes { get; set; } = [];

    public bool Enabled { get; set; }
}
