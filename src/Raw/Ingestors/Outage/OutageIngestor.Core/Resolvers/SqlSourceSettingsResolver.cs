using Microsoft.Data.SqlClient;
using OutageIngestor.Configuration;

namespace OutageIngestor.Resolvers;

/// <summary>Resolves SqlSourceOptions (keyed configuration, falling back to a legacy connection string) into a
/// connection string and optional token-auth service principal. Outage shares the same DART Synapse source
/// as Production (`cat_gold` schema), keyed here as "OutageDatabase".</summary>
public static class SqlSourceSettingsResolver
{
    public static (string ConnectionString, SourceServicePrincipalOptions? Principal) Resolve(
        SqlSourceOptions options, string? legacyConnectionString)
    {
        if (options.ConnectionStrings.Count == 0)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(legacyConnectionString);
            ParseConnectionString(legacyConnectionString);
            return (legacyConnectionString, null);
        }

        if (options.ConnectionStrings.Select(entry => entry.Key).Distinct(StringComparer.Ordinal).Count() != options.ConnectionStrings.Count
            || options.ServicePrincipals.Select(entry => entry.Key).Distinct(StringComparer.Ordinal).Count() != options.ServicePrincipals.Count)
            throw new ArgumentException("Source connection and service principal keys must be unique.");

        var connection = options.ConnectionStrings.SingleOrDefault(entry => entry.Key == "OutageDatabase");
        if (connection is null || !connection.Enabled)
            throw new ArgumentException("An enabled OutageDatabase connection is required.");

        ArgumentException.ThrowIfNullOrWhiteSpace(connection.ServicePrincipalKey);
        var principal = options.ServicePrincipals.SingleOrDefault(entry => entry.Key == connection.ServicePrincipalKey);
        if (principal is null || !principal.Enabled)
            throw new ArgumentException("OutageDatabase must reference an enabled service principal.");

        ArgumentException.ThrowIfNullOrWhiteSpace(options.Azure.TenantId);
        ArgumentException.ThrowIfNullOrWhiteSpace(principal.ClientId);
        ArgumentException.ThrowIfNullOrWhiteSpace(principal.Secret);
        if (!Uri.TryCreate(principal.Resource, UriKind.Absolute, out var resource)
            || resource.Scheme != Uri.UriSchemeHttps || string.IsNullOrEmpty(resource.Host)
            || !string.IsNullOrEmpty(resource.Query) || !string.IsNullOrEmpty(resource.Fragment)
            || !string.IsNullOrEmpty(resource.UserInfo))
            throw new ArgumentException("The source service principal resource must be an absolute HTTPS resource URI.");
        ArgumentOutOfRangeException.ThrowIfNegative(principal.ExpirationThresholdInMinutes);

        ArgumentException.ThrowIfNullOrWhiteSpace(connection.Value);
        var sql = ParseConnectionString(connection.Value);
        if (sql.IntegratedSecurity || sql.Authentication != SqlAuthenticationMethod.NotSpecified
            || !string.IsNullOrEmpty(sql.UserID) || !string.IsNullOrEmpty(sql.Password))
            throw new ArgumentException("OutageDatabase cannot combine token authentication with connection-string credentials or Authentication.");

        return (connection.Value, principal);
    }

    private static SqlConnectionStringBuilder ParseConnectionString(string value)
    {
        try
        {
            var connection = new SqlConnectionStringBuilder(value);
            ArgumentException.ThrowIfNullOrWhiteSpace(connection.DataSource);
            return connection;
        }
        catch (ArgumentException)
        {
            throw new ArgumentException("The outage SQL connection string is invalid.");
        }
    }
}
