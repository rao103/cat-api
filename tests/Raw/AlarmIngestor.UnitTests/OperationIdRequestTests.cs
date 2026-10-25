using AlarmIngestor.Worker.Configuration;

namespace AlarmIngestor.UnitTests;

public sealed class OperationIdRequestTests
{
    [Fact]
    public void ParserPreservesSuppliedOperationId()
    {
        const string operationId = "bb2e7ced-3d1c-43f9-9535-de6217640b76";

        var request = IngestionRequestParser.Parse("1", "2", "refresh-run", "Historical", "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z", operationId);

        Assert.Equal(operationId, request.OperationId);
        Assert.Equal("2", request.WindfarmId);
        Assert.Equal("refresh-run", request.RunId);
    }

    [Fact]
    public void ScheduledRequestLeavesOperationCreationToRepository()
    {
        var request = IngestionRequestParser.Parse("1", null, null, null, null, null);

        Assert.Null(request.OperationId);
    }

    [Theory]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("op\ninvalid")]
    public void ParserRejectsInvalidOperationId(string operationId)
    {
        Assert.Throws<InvalidDataException>(() =>
            IngestionRequestParser.Parse("1", null, null, null, null, null, operationId));
    }

    [Fact]
    public void ParserRejectsOversizedOperationId()
    {
        Assert.Throws<InvalidDataException>(() =>
            IngestionRequestParser.Parse("1", null, null, null, null, null, new string('x', 257)));
    }
}
