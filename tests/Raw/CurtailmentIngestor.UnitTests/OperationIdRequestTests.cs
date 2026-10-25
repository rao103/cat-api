using CurtailmentIngestor.Worker.Configuration;

namespace CurtailmentIngestor.UnitTests;

public sealed class OperationIdRequestTests
{
    [Fact]
    public void ParserPreservesSuppliedOperationId()
    {
        const string operationId = "bb2e7ced-3d1c-43f9-9535-de6217640b76";

        var request = IngestionRequestParser.Parse("1", "2", "refresh-run", operationId);

        Assert.Equal(operationId, request.OperationId);
        Assert.Equal("2", request.WindfarmId);
        Assert.Equal("refresh-run", request.RunId);
    }

    [Fact]
    public void ScheduledRequestLeavesOperationCreationToRepository()
    {
        var request = IngestionRequestParser.Parse("1", null, null);

        Assert.Null(request.OperationId);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("op\ninvalid")]
    public void ParserRejectsInvalidOperationId(string operationId)
    {
        Assert.Throws<InvalidDataException>(() =>
            IngestionRequestParser.Parse("1", null, null, operationId));
    }

    [Fact]
    public void ParserRejectsOversizedOperationId()
    {
        Assert.Throws<InvalidDataException>(() =>
            IngestionRequestParser.Parse("1", null, null, new string('x', 257)));
    }
}
