using AlarmIngestor.Repositories;

namespace AlarmIngestor.UnitTests;

/// <summary>
/// Explicit test of the "windfarm INTEGER bind from public string positive int" assumption:
/// only <see cref="PostgresExtractionCursorRepository"/>'s DB boundary forces the windfarm
/// identifier to a positive integer, using invariant-culture parsing so a locale with different
/// digit/group separators can never change behavior.
/// </summary>
public sealed class PostgresExtractionCursorRepositoryWindfarmIdTests
{
    [Theory]
    [InlineData("1")]
    [InlineData("42")]
    [InlineData("1000000")]
    public void PositiveIntegerTextParsesToTheSameInt(string text)
    {
        Assert.Equal(int.Parse(text), PostgresExtractionCursorRepository.ParseWindfarmId(text));
    }

    [Theory]
    [InlineData("")]
    [InlineData("0")]
    [InlineData("-1")]
    [InlineData("farm")]
    [InlineData("1.5")]
    [InlineData(" 1")]
    [InlineData("+1")]
    public void NonPositiveOrNonNumericTextThrows(string text)
    {
        Assert.Throws<ArgumentException>(() => PostgresExtractionCursorRepository.ParseWindfarmId(text));
    }
}
