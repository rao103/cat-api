using System.Data.Common;

namespace OutageIngestor.Interfaces;

public interface IDatabaseConnectionFactory
{
    DbConnection CreateConnection();
}
