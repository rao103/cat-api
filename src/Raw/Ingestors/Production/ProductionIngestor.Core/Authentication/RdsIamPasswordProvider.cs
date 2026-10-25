using Amazon;
using Amazon.RDS.Util;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using ProductionIngestor.Configuration;
using ProductionIngestor.Validators;

namespace ProductionIngestor.Authentication;

public sealed class RdsIamPasswordProvider
{
    private readonly Func<CancellationToken, Task<string>> _generateToken;

    public RdsIamPasswordProvider(ControlPlaneOptions options,
        Func<CancellationToken, Task<string>>? generateToken = null)
    {
        ControlPlaneOptionsValidator.ValidateAndThrow(options);
        if (!string.Equals(options.Authentication, "AwsIam", StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("RDS IAM password generation requires AwsIam authentication.");
        var region = RegionEndpoint.GetBySystemName(options.Region);
        var signingHost = options.SigningHost;
        var signingPort = options.SigningPort;
        var username = options.Username;
        var profileName = options.AwsProfile;
        var profileCredentials = new Lazy<AWSCredentials>(() =>
        {
            if (!new CredentialProfileStoreChain().TryGetAWSCredentials(profileName, out var credentials))
                throw new InvalidOperationException("The configured control-plane AWS profile could not be loaded. Configure it and sign in with AWS SSO.");
            return credentials;
        });
        _generateToken = generateToken ?? (cancellationToken =>
            (string.IsNullOrWhiteSpace(profileName)
                ? RDSAuthTokenGenerator.GenerateAuthTokenAsync(region, signingHost, signingPort, username)
                : RDSAuthTokenGenerator.GenerateAuthTokenAsync(profileCredentials.Value, region, signingHost, signingPort, username))
            .WaitAsync(cancellationToken));
    }

    public async ValueTask<string> GetPasswordAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var token = await _generateToken(cancellationToken).WaitAsync(cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        if (string.IsNullOrWhiteSpace(token))
            throw new InvalidOperationException("The RDS IAM token provider returned an empty token.");
        return token;
    }
}