namespace CAT.RawFanOut.Configuration;

public sealed class ControlPlaneOptions
{
    public const string SectionName = "ControlPlane";

    public string Provider { get; set; } = "PostgreSql";
    public string Authentication { get; set; } = "AwsIam";
    public string Host { get; set; } = string.Empty;
    public int Port { get; set; } = 5432;
    public string Database { get; set; } = string.Empty;
    public string Username { get; set; } = string.Empty;
    public string Region { get; set; } = "eu-north-1";
    public string AwsProfile { get; set; } = string.Empty;
    public string SigningHost { get; set; } = string.Empty;
    public int SigningPort { get; set; } = 5432;
    public string RootCertificate { get; set; } = string.Empty;
}