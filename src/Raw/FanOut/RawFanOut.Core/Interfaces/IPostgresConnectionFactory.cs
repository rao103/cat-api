using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace CAT.RawFanOut.Interfaces;

public interface IPostgresConnectionFactory
{
    ValueTask<NpgsqlConnection> OpenConnectionAsync(CancellationToken cancellationToken);
}