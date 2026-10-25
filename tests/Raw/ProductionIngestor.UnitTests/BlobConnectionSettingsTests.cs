using Azure.Core;
using Microsoft.Extensions.Options;
using ProductionIngestor.Adapters.Storage;
using ProductionIngestor.Configuration;
using ProductionIngestor.Resolvers;
using ProductionIngestor.Validators;

namespace ProductionIngestor.UnitTests;

public sealed class BlobConnectionSettingsTests
{
    private static SourceOptions CreateOptions() => new()
    {
        SourceSystemName = "production",
        BlobContainerName = "oi-cat",
        ControlPlaneConnectionString = "Host=cp.example;Database=control;",
        SqsQueueUrl = "https://sqs.eu-north-1.amazonaws.com/123456789012/queue",
    };

    [Fact]
    public void ConnectionStringTakesPrecedenceOverServiceUri()
    {
        var options = CreateOptions();
        options.BlobConnectionString = "UseDevelopmentStorage=true";
        options.BlobServiceUri = "https://example.blob.core.windows.net";

        var resolved = BlobConnectionSettingsResolver.Resolve(options);

        Assert.True(resolved.UsesConnectionString);
        Assert.Equal("UseDevelopmentStorage=true", resolved.ConnectionString);
        Assert.Equal("oi-cat", resolved.ContainerName);
    }

    [Fact]
    public void ServiceUriIsUsedWhenNoConnectionStringIsConfigured()
    {
        var options = CreateOptions();
        options.BlobServiceUri = "https://orsteddatalakedevraw2.blob.core.windows.net";

        var resolved = BlobConnectionSettingsResolver.Resolve(options);

        Assert.False(resolved.UsesConnectionString);
        Assert.Equal(new Uri("https://orsteddatalakedevraw2.blob.core.windows.net"), resolved.ServiceUri);
    }

    [Fact]
    public void MissingBothConnectionStringAndServiceUriFails()
    {
        var options = CreateOptions();
        Assert.Throws<ArgumentException>(() => BlobConnectionSettingsResolver.Resolve(options));
    }

    [Theory]
    [InlineData("not-a-uri")]
    [InlineData("http://example.blob.core.windows.net")]
    [InlineData("ftp://example.blob.core.windows.net")]
    public void MalformedOrNonHttpsServiceUriFails(string uri)
    {
        var options = CreateOptions();
        options.BlobServiceUri = uri;
        Assert.Throws<ArgumentException>(() => BlobConnectionSettingsResolver.Resolve(options));
    }

    [Fact]
    public void MissingContainerNameFailsRegardlessOfBlobAccessMode()
    {
        var options = CreateOptions();
        options.BlobContainerName = "";
        options.BlobConnectionString = "UseDevelopmentStorage=true";
        Assert.Throws<ArgumentException>(() => BlobConnectionSettingsResolver.Resolve(options));
    }

    [Fact]
    public void ObjectStoreCanBeConstructedFromEitherResolvedMode()
    {
        var connectionStringStore = new AzureBlobRawObjectStore("UseDevelopmentStorage=true", "oi-cat");
        Assert.NotNull(connectionStringStore);

        var uriStore = new AzureBlobRawObjectStore(new Uri("https://orsteddatalakedevraw2.blob.core.windows.net"),
            new NoopTokenCredential(), "oi-cat");
        Assert.NotNull(uriStore);
    }

    [Fact]
    public void SourceOptionsValidatorAcceptsServiceUriConfigurationOffline()
    {
        var options = CreateOptions();
        options.BlobServiceUri = "https://orsteddatalakedevraw2.blob.core.windows.net";
        options.ConnectionString = "Data Source=source.example;Initial Catalog=production;";

        SourceOptionsValidator.ValidateAndThrow(options, new SqlSourceOptions());
    }

    [Fact]
    public void SourceOptionsValidatorRejectsMissingBlobConfigurationOffline()
    {
        var options = CreateOptions();
        options.ConnectionString = "Data Source=source.example;Initial Catalog=production;";

        Assert.Throws<ArgumentException>(() => SourceOptionsValidator.ValidateAndThrow(options, new SqlSourceOptions()));
    }

    private sealed class NoopTokenCredential : TokenCredential
    {
        private static readonly AccessToken Token = new("test-token", DateTimeOffset.MaxValue);

        public override AccessToken GetToken(TokenRequestContext requestContext, CancellationToken cancellationToken) => Token;

        public override ValueTask<AccessToken> GetTokenAsync(TokenRequestContext requestContext, CancellationToken cancellationToken) =>
            ValueTask.FromResult(Token);
    }
}
