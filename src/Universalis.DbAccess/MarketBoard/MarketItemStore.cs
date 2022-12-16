using System;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Microsoft.Extensions.Logging;
using Universalis.Entities.MarketBoard;

namespace Universalis.DbAccess.MarketBoard;

public class MarketItemStore : IMarketItemStore
{
    private readonly ILogger<MarketItemStore> _logger;
    private readonly IMapper _mapper;

    public MarketItemStore(ICluster cluster, ILogger<MarketItemStore> logger)
    {
        _logger = logger;

        var scylla = cluster.Connect();
        scylla.CreateKeyspaceIfNotExists("market_item");
        scylla.ChangeKeyspace("market_item");
        var table = scylla.GetTable<MarketItem>();
        table.CreateIfNotExists();

        _mapper = new Mapper(scylla);
    }

    public async Task Insert(MarketItem marketItem, CancellationToken cancellationToken = default)
    {
        if (marketItem == null)
        {
            throw new ArgumentNullException(nameof(marketItem));
        }

        try
        {
            await _mapper.InsertAsync(marketItem);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Failed to insert market item (world={WorldId}, item={ItemId})", marketItem.WorldId, marketItem.ItemId);
            throw;
        }
    }

    public async Task Update(MarketItem marketItem, CancellationToken cancellationToken = default)
    {
        if (marketItem == null)
        {
            throw new ArgumentNullException(nameof(marketItem));
        }

        try
        {
            await _mapper.InsertAsync(marketItem);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Failed to insert market item (world={WorldId}, item={ItemId})", marketItem.WorldId, marketItem.ItemId);
            throw;
        }
    }

    public Task<MarketItem> Retrieve(int worldId, int itemId, CancellationToken cancellationToken = default)
    {
        // Fetch data from the database
        return _mapper.FirstOrDefaultAsync<MarketItem>("SELECT * FROM market_item WHERE item_id=? AND world_id=?", itemId, worldId);
    }
}