using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Universalis.DbAccess.Queries.MarketBoard;
using Universalis.Entities.MarketBoard;

namespace Universalis.DbAccess.MarketBoard;

public class HistoryDbAccess : IHistoryDbAccess
{
    private readonly IMarketItemStore _marketItemStore;
    private readonly ISaleStore _saleStore;

    public HistoryDbAccess(IMarketItemStore marketItemStore, ISaleStore saleStore)
    {
        _marketItemStore = marketItemStore;
        _saleStore = saleStore;
    }

    public async Task Create(History document, CancellationToken cancellationToken = default)
    {
        await _marketItemStore.Insert(new MarketItem
        {
            WorldId = document.WorldId,
            ItemId = document.ItemId,
            LastUploadTime =
                DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(document.LastUploadTimeUnixMilliseconds)).UtcDateTime,
        }, cancellationToken);
        await _saleStore.InsertMany(document.Sales, cancellationToken);
    }

    public async Task<History> Retrieve(HistoryQuery query, CancellationToken cancellationToken = default)
    {
        var marketItem = await _marketItemStore.Retrieve(query.WorldId, query.ItemId, cancellationToken);
        if (marketItem == null)
        {
            return null;
        }
        
        var sales = await _saleStore.RetrieveBySaleTime(query.WorldId, query.ItemId, query.Count ?? 1000, cancellationToken: cancellationToken);
        return new History
        {
            WorldId = marketItem.WorldId,
            ItemId = marketItem.ItemId,
            LastUploadTimeUnixMilliseconds = new DateTimeOffset(marketItem.LastUploadTime).ToUnixTimeMilliseconds(),
            Sales = sales.ToList(),
        };
    }

    public async IAsyncEnumerable<History> RetrieveMany(HistoryManyQuery query, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var worldId in query.WorldIds)
        {
            var result = await Retrieve(new HistoryQuery { WorldId = worldId, ItemId = query.ItemId, Count = query.Count }, cancellationToken);
            if (result != null)
                yield return result;
        }
    }

    public async Task InsertSales(IEnumerable<Sale> sales, HistoryQuery query, CancellationToken cancellationToken = default)
    {
        await _marketItemStore.Update(new MarketItem
        {
            WorldId = query.WorldId,
            ItemId = query.ItemId,
            LastUploadTime = DateTime.UtcNow,
        }, cancellationToken);
        await _saleStore.InsertMany(sales, cancellationToken);
    }
}