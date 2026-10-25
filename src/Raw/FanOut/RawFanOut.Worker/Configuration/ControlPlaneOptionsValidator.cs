#nullable enable
using System;
using System.IO;
using CAT.RawFanOut.Configuration;
using Microsoft.Extensions.Options;

namespace CAT.RawFanOut.Worker.Configuration;

public sealed class ControlPlaneOptionsValidator : IValidateOptions<ControlPlaneOptions>
{
    public ValidateOptionsResult Validate(string? name, ControlPlaneOptions options)
    {
        if (!string.Equals(options.Provider, "PostgreSql", StringComparison.OrdinalIgnoreCase))
            return ValidateOptionsResult.Fail("ControlPlane:Provider must be PostgreSql.");
        if (!string.Equals(options.Authentication, "AwsIam", StringComparison.OrdinalIgnoreCase))
            return ValidateOptionsResult.Fail("ControlPlane:Authentication must be AwsIam.");
        if (string.IsNullOrWhiteSpace(options.Host) ||
            !string.Equals(options.Host, options.SigningHost, StringComparison.OrdinalIgnoreCase))
            return ValidateOptionsResult.Fail("ControlPlane:Host must be the real RDS hostname and match SigningHost.");
        if (options.Port is < 1 or > 65535 || options.SigningPort is < 1 or > 65535)
            return ValidateOptionsResult.Fail("ControlPlane ports must be between 1 and 65535.");
        if (string.IsNullOrWhiteSpace(options.Database) || string.IsNullOrWhiteSpace(options.Username) ||
            string.IsNullOrWhiteSpace(options.Region))
            return ValidateOptionsResult.Fail("ControlPlane database, username, and region are required.");

        var rootCertificate = Environment.ExpandEnvironmentVariables(options.RootCertificate);
        if (string.IsNullOrWhiteSpace(rootCertificate) || !Path.IsPathRooted(rootCertificate) ||
            !File.Exists(rootCertificate))
            return ValidateOptionsResult.Fail("ControlPlane:RootCertificate must be an existing trusted RDS CA file.");

        return ValidateOptionsResult.Success;
    }
}