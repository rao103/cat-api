using Microsoft.Extensions.Options;
using Npgsql;
using OutageIngestor.Configuration;
using OutageIngestor.Core.Constants;

namespace OutageIngestor.Validators;

public sealed class ControlPlaneOptionsValidator : IValidateOptions<ControlPlaneOptions>
{
    public ValidateOptionsResult Validate(string? name, ControlPlaneOptions options)
    {
        try
        {
            ValidateAndThrow(options);
            return ValidateOptionsResult.Success;
        }
        catch (ArgumentException exception)
        {
            return ValidateOptionsResult.Fail(exception.Message);
        }
    }

    internal static void ValidateAndThrow(ControlPlaneOptions options)
    {
        if (!string.Equals(options.Provider, Constant.PostgreSql, StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("The cursor repository requires PostgreSql. AzureSql requires its own cursor repository and schema.");

        if (string.Equals(options.Authentication, Constant.ConnectionString, StringComparison.OrdinalIgnoreCase))
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(options.ConnectionString);
            _ = new NpgsqlConnectionStringBuilder(options.ConnectionString);
            return;
        }

        if (!string.Equals(options.Authentication, Constant.AwsIam, StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("ControlPlane:Authentication must be AwsIam or ConnectionString.");

        ValidateAwsIamConfiguration(options);
    }

    private static void ValidateAwsIamConfiguration(ControlPlaneOptions options)
    {
        if (!string.IsNullOrEmpty(options.ConnectionString))
            throw new ArgumentException("ControlPlane:ConnectionString must be empty in AwsIam mode; password overrides are not supported.");

        ArgumentException.ThrowIfNullOrWhiteSpace(options.Host);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.SigningHost);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.Database);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.Username);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.Region);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.RootCertificate);

        if (options.Port is < 1 or > 65535 || options.SigningPort is < 1 or > 65535)
            throw new ArgumentException("Control-plane connection and signing ports must be between 1 and 65535.");

        if (Uri.CheckHostName(options.SigningHost) != UriHostNameType.Dns ||
            !options.SigningHost.EndsWith($".{options.Region}.rds.amazonaws.com", StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("SigningHost must be the real RDS endpoint in the configured region.");

        if (!string.Equals(options.Host, options.SigningHost, StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("Host must match SigningHost for TLS hostname verification; use a local hosts mapping for tunnels.");

        if (!File.Exists(options.RootCertificate))
            throw new ArgumentException("ControlPlane:RootCertificate must refer to an installed RDS CA bundle.");
    }
}
