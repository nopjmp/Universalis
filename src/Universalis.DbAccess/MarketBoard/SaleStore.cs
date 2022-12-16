using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Universalis.Entities.MarketBoard;

namespace Universalis.DbAccess.MarketBoard;

public class SaleStore : ISaleStore
{
    private readonly ICacheRedisMultiplexer _cache;
    private readonly ILogger<SaleStore> _logger;
    private readonly ISession _scylla;
    private readonly IMapper _mapper;

    private readonly PreparedStatement _insertStatement;

    public SaleStore(ICluster scylla, ICacheRedisMultiplexer cache, ILogger<SaleStore> logger)
    {
        _cache = cache;
        _logger = logger;

        _scylla = scylla.Connect();
        _scylla.CreateKeyspaceIfNotExists("sale");
        _scylla.ChangeKeyspace("sale");
        var table = _scylla.GetTable<Sale>();
        table.CreateIfNotExists();

        _mapper = new Mapper(_scylla);

        _insertStatement = _scylla.Prepare("" +
            "INSERT INTO sale" +
            "(id, sale_time, item_id, world_id, buyer_name, hq, on_mannequin, quantity, unit_price, uploader_id)" +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
    }

    public async Task Insert(Sale sale, CancellationToken cancellationToken = default)
    {
        if (sale == null)
        {
            throw new ArgumentNullException(nameof(sale));
        }

        if (sale.BuyerName == null)
        {
            throw new ArgumentException("Sale buyer name may not be null.", nameof(sale));
        }

        if (sale.Quantity == null)
        {
            throw new ArgumentException("Sale quantity may not be null.", nameof(sale));
        }

        if (sale.OnMannequin == null)
        {
            throw new ArgumentException("Mannequin state may not be null.", nameof(sale));
        }

        try
        {
            await _mapper.InsertAsync(sale);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Failed to insert sale (world={WorldId}, item={ItemId})", sale.WorldId, sale.ItemId);
            throw;
        }
    }

    public async Task InsertMany(IEnumerable<Sale> sales, CancellationToken cancellationToken = default)
    {
        if (sales == null)
        {
            throw new ArgumentNullException(nameof(sales));
        }

        var batch = new BatchStatement();
        foreach (var sale in sales)
        {
            var bound = _insertStatement.Bind(
                sale.Id,
                sale.SaleTime,
                sale.ItemId,
                sale.WorldId,
                sale.BuyerName,
                sale.Hq,
                sale.OnMannequin,
                sale.Quantity,
                sale.PricePerUnit,
                sale.UploaderIdHash);
            batch.Add(bound);
        }

        try
        {
            await _scylla.ExecuteAsync(batch);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Failed to insert sales");
            throw;
        }
    }

    public async Task<IEnumerable<Sale>> RetrieveBySaleTime(int worldId, int itemId, int count, DateTime? from = null, CancellationToken cancellationToken = default)
    {
        var sales = Enumerable.Empty<Sale>();
        if (count == 0)
        {
            return sales;
        }

        // Fetch data from the database
        var timestamp = from == null ? 0 : new DateTimeOffset(from.Value).ToUnixTimeMilliseconds();
        try
        {
            sales = await _mapper.FetchAsync<Sale>("SELECT * FROM sale WHERE item_id=? AND world_id=? AND sale_time>=? ORDER BY sale_time DESC LIMIT ?", itemId, worldId, timestamp, count);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Failed to retrieve sales (world={WorldId}, item={ItemId})", worldId, itemId);
            throw;
        }

        return sales
            .Select(sale =>
            {
                sale.SaleTime = DateTime.SpecifyKind(sale.SaleTime, DateTimeKind.Utc);
                return sale;
            });
    }

    public async Task<long> RetrieveUnitTradeVolume(int worldId, int itemId, DateTime from, DateTime to, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _mapper.FirstAsync<long>("SELECT SUM(quantity) FROM sale WHERE item_id=? AND world_id=? AND sale_time>= ? AND sale_time <= ? ORDER BY sale_time", itemId, worldId, from, to);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Failed to retrieve unit trade volume (world={WorldId}, item={ItemId})", worldId, itemId);
            throw;
        }
    }

    public async Task<long> RetrieveGilTradeVolume(int worldId, int itemId, DateTime from, DateTime to, CancellationToken cancellationToken = default)
    {
        try
        {
            return await _mapper.FirstAsync<long>("SELECT SUM(unit_price) FROM sale WHERE item_id=? AND world_id=? AND sale_time>= ? AND sale_time <= ? ORDER BY sale_time", itemId, worldId, from, to);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Failed to retrieve gil trade volume (world={WorldId}, item={ItemId})", worldId, itemId);
            throw;
        }
    }
}