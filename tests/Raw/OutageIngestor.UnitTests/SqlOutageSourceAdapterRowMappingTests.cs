using System.Data;
using OutageIngestor.Adapters.Source;

namespace OutageIngestor.UnitTests;

/// <summary>
/// Covers <see cref="SqlOutageSourceAdapter.ReadLiveRow"/> mapping directly against an
/// <see cref="IDataReader"/> (a <see cref="DataTableReader"/>, not a live SQL connection):
/// full classification-attribute mapping, and identity columns typed as
/// <see cref="Guid"/> rather than <see cref="string"/> — the legacy integrator's mapper never
/// read <c>OutageID</c> at all, so a plain <c>GetString</c> assumption was never exercised.
/// </summary>
public sealed class SqlOutageSourceAdapterRowMappingTests
{
    private static DataTable BuildLiveTable()
    {
        var table = new DataTable();
        table.Columns.Add("SiteRDSPPID", typeof(string));
        table.Columns.Add("WtgRDSPPID", typeof(string));
        table.Columns.Add("OutageID", typeof(Guid));
        table.Columns.Add("OutageClassificationID", typeof(string));
        table.Columns.Add("OutageClassificationStartUTC", typeof(DateTime));
        table.Columns.Add("OutageClassificationEndUTC", typeof(DateTime));
        table.Columns.Add("OutageClassificationComment", typeof(string));
        table.Columns.Add("OutageClassificationAlarmCode", typeof(string));
        table.Columns.Add("OutageClassificationAlarmText", typeof(string));
        table.Columns.Add("OutageClassificationCategory", typeof(string));
        table.Columns.Add("OutageClassificationSubCategory", typeof(string));
        table.Columns.Add("LastUpdatedUTC", typeof(DateTime));
        return table;
    }

    [Fact]
    public void ReadLiveRowMapsFullClassificationAttributes()
    {
        var table = BuildLiveTable();
        var start = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Unspecified);
        var end = start.AddHours(2);
        var lastUpdated = start.AddHours(3);
        var outageId = Guid.NewGuid();
        table.Rows.Add("site-1", "wtg-1", outageId, "OC-1", start, end,
            "WFR comment", "A100", "Alarm text", "Grid", "Voltage dip", lastUpdated);

        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());
        var evt = SqlOutageSourceAdapter.ReadLiveRow(reader);

        Assert.Equal(outageId.ToString(), evt.OutageId);
        Assert.Equal("OC-1", evt.EventId);
        Assert.Equal("site-1", evt.WindfarmId);
        Assert.Equal("wtg-1", evt.ItemId);
        Assert.Equal("WFR comment", evt.Comment);
        Assert.Equal("A100", evt.AlarmCode);
        Assert.Equal("Alarm text", evt.AlarmText);
        Assert.Equal("Grid", evt.Category);
        Assert.Equal("Voltage dip", evt.SubCategory);
        Assert.Equal("Closed", evt.Status);
    }

    [Fact]
    public void ReadLiveRowMapsNullClassificationAttributesAsNull()
    {
        var table = BuildLiveTable();
        var start = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Unspecified);
        table.Rows.Add("site-1", "wtg-1", Guid.NewGuid(), "OC-2", start, DBNull.Value,
            DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, start);

        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());
        var evt = SqlOutageSourceAdapter.ReadLiveRow(reader);

        Assert.Null(evt.Comment);
        Assert.Null(evt.AlarmCode);
        Assert.Null(evt.AlarmText);
        Assert.Null(evt.Category);
        Assert.Null(evt.SubCategory);
        Assert.Null(evt.OriginalEndUtc);
        Assert.Equal("Open", evt.Status);
    }

    [Fact]
    public void ReadIdentityStringConvertsAGuidColumnInvariantly()
    {
        var table = new DataTable();
        table.Columns.Add("OutageID", typeof(Guid));
        var id = Guid.NewGuid();
        table.Rows.Add(id);

        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());

        Assert.Equal(id.ToString(), SqlOutageSourceAdapter.ReadIdentityString(reader, "OutageID"));
    }

    [Fact]
    public void ReadIdentityStringReturnsEmptyForNull()
    {
        var table = new DataTable();
        table.Columns.Add("OutageID", typeof(string));
        table.Rows.Add(DBNull.Value);

        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());

        Assert.Equal(string.Empty, SqlOutageSourceAdapter.ReadIdentityString(reader, "OutageID"));
    }
}
