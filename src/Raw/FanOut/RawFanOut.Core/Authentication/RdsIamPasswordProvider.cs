using System;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.RDS.Util;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using CAT.RawFanOut.Configuration;

namespace CAT.RawFanOut.Authentication;

public sealed class RdsIamPasswordProvider
{
    private readonly ControlPlaneOptions options;
    private readonly Lazy<AWSCredentials>? profileCredentials;

    public RdsIamPasswordProvider(ControlPlaneOptions options)
    {
        this.options = options;
        if (!string.IsNullOrWhiteSpace(options.AwsProfile))
        {
            profileCredentials = new Lazy<AWSCredentials>(() =>
            {
                if (!new CredentialProfileStoreChain().TryGetAWSCredentials(options.AwsProfile, out var credentials))
                    throw new InvalidOperationException("The configured control-plane AWS profile could not be loaded. Configure and sign in to it.");
                return credentials;
            });
        }
    }

    public async ValueTask<string> GetPasswordAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var region = RegionEndpoint.GetBySystemName(options.Region);
        var tokenTask = profileCredentials is null
            ? RDSAuthTokenGenerator.GenerateAuthTokenAsync(
                region, options.SigningHost, options.SigningPort, options.Username)
            : RDSAuthTokenGenerator.GenerateAuthTokenAsync(
                profileCredentials.Value, region, options.SigningHost, options.SigningPort, options.Username);
        var token = await tokenTask.WaitAsync(cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        if (string.IsNullOrWhiteSpace(token))
            throw new InvalidOperationException("RDS IAM token generation returned an empty token.");
        return token;
    }
}