using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Universalis.Application.Common;
using Universalis.Application.Views.V1;
using Universalis.DataTransformations;
using Universalis.DbAccess.MarketBoard;
using Universalis.DbAccess.Queries.MarketBoard;
using Universalis.GameData;

namespace Universalis.Application.Controllers;

public class HistoryControllerBase : WorldDcRegionControllerBase
{
    protected readonly IHistoryDbAccess History;
    
    public HistoryControllerBase(IGameDataProvider gameData, IHistoryDbAccess historyDb) : base(gameData)
    {
        History = historyDb;
    }

    protected async Task<(bool, HistoryView)> GetHistoryView(
        WorldDcRegion worldDcRegion,
        int[] worldIds,
        int itemId,
        int entries,
        long statsWithin = 604800000,
        long entriesWithin = -1,
        CancellationToken cancellationToken = default)
    {
        // Fetch the data
        var data = History.RetrieveMany(new HistoryManyQuery
        {
            WorldIds = worldIds,
            ItemId = itemId,
            Count = entries,
        }, cancellationToken);
        var resolved = await data.AnyAsync();
        var worlds = GameData.AvailableWorlds();

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var nowSeconds = now / 1000;
        var history = await data
            .Where(o => worlds.ContainsKey(o.WorldId))
            .AggregateAwaitAsync(new HistoryView(), (agg, next) =>
            {
                agg.Sales.AddRange(next.Sales
                    .Where(s => entriesWithin < 0 || nowSeconds - new DateTimeOffset(s.SaleTime).ToUnixTimeSeconds() < entriesWithin)
                    .Where(s => s.Quantity is > 0)
                    .Select(s => new MinimizedSaleView
                    {
                        Hq = s.Hq,
                        PricePerUnit = s.PricePerUnit,
                        Quantity = s.Quantity ?? 0, // This should never be 0 since we're filtering out null and zero quantities
                        BuyerName = s.BuyerName,
                        OnMannequin = s.OnMannequin,
                        TimestampUnixSeconds = new DateTimeOffset(s.SaleTime).ToUnixTimeSeconds(),
                        WorldId = !worldDcRegion.IsWorld ? next.WorldId : null,
                        WorldName = !worldDcRegion.IsWorld ? worlds[next.WorldId] : null,
                    }));
                agg.LastUploadTimeUnixMilliseconds = (long)Math.Max(next.LastUploadTimeUnixMilliseconds, agg.LastUploadTimeUnixMilliseconds);

                return ValueTask.FromResult(agg);
            }, cancellationToken);

        history.Sales = history.Sales.OrderByDescending(s => s.TimestampUnixSeconds).Take(entries).ToList();

        var nqSales = history.Sales.Where(s => !s.Hq).ToList();
        var hqSales = history.Sales.Where(s => s.Hq).ToList();

        return (resolved, new HistoryView
        {
            Sales = history.Sales.Take(entries).ToList(),
            ItemId = itemId,
            WorldId = worldDcRegion.IsWorld ? worldDcRegion.WorldId : null,
            WorldName = worldDcRegion.IsWorld ? worldDcRegion.WorldName : null,
            DcName = worldDcRegion.IsDc ? worldDcRegion.DcName : null,
            RegionName = worldDcRegion.IsRegion ? worldDcRegion.RegionName : null,
            LastUploadTimeUnixMilliseconds = history.LastUploadTimeUnixMilliseconds,
            StackSizeHistogram = Statistics.GetDistribution(history.Sales.Select(s => s.Quantity)),
            StackSizeHistogramNq = Statistics.GetDistribution(nqSales.Select(s => s.Quantity)),
            StackSizeHistogramHq = Statistics.GetDistribution(hqSales.Select(s => s.Quantity)),
            SaleVelocity = Statistics.VelocityPerDay(history.Sales.Select(s => s.TimestampUnixSeconds * 1000), now, statsWithin),
            SaleVelocityNq = Statistics.VelocityPerDay(nqSales.Select(s => s.TimestampUnixSeconds * 1000), now, statsWithin),
            SaleVelocityHq = Statistics.VelocityPerDay(hqSales.Select(s => s.TimestampUnixSeconds * 1000), now, statsWithin),
        });
    }
}