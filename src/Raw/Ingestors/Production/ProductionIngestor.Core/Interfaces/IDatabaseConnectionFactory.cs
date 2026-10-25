using System.Data.Common;

namespace ProductionIngestor.Interfaces;

public interface IDatabaseConnectionFactory
{
    DbConnection CreateConnection();
}