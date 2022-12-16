using System.Threading;
using System.Threading.Tasks;
using Universalis.DbAccess.Queries.MarketBoard;

namespace Universalis.DbAccess.MarketBoard;

public interface ISaleStatisticsDbAccess
{
    public ValueTask<long> RetrieveUnitTradeVolume(TradeVolumeQuery query,
        CancellationToken cancellationToken = default);

    public ValueTask<long> RetrieveGilTradeVolume(TradeVolumeQuery query,
        CancellationToken cancellationToken = default);
}
