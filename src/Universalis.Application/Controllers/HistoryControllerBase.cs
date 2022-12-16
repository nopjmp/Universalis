using System;
using System.Collections.Generic;
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

    protected async Task<HistoryView> GetHistoryView(
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
        var worlds = GameData.AvailableWorlds();

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var nowSeconds = now / 1000;
        var (lastUploadTimeUnixMilliseconds, salesHistory) = await data
            .Where(o => worlds.ContainsKey(o.WorldId))
            .AggregateAwaitAsync((0L, new List<MinimizedSaleView>(entries)), (agg, next) =>
            {
                var (uploadTimeUnixMilliseconds, saleViews) = agg;
                saleViews.AddRange(next.Sales
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
                uploadTimeUnixMilliseconds = (long)Math.Max(next.LastUploadTimeUnixMilliseconds, uploadTimeUnixMilliseconds);

                return ValueTask.FromResult((uploadTimeUnixMilliseconds, saleViews));
            }, cancellationToken);

        salesHistory = salesHistory.OrderByDescending(s => s.TimestampUnixSeconds)
            .Take(entries)
            .ToList();

        var nqSales = salesHistory.Where(s => !s.Hq).ToList();
        var hqSales = salesHistory.Where(s => s.Hq).ToList();

        return new HistoryView
        {
            Sales = salesHistory.ToList(),
            ItemId = itemId,
            WorldId = worldDcRegion.IsWorld ? worldDcRegion.WorldId : null,
            WorldName = worldDcRegion.IsWorld ? worldDcRegion.WorldName : null,
            DcName = worldDcRegion.IsDc ? worldDcRegion.DcName : null,
            RegionName = worldDcRegion.IsRegion ? worldDcRegion.RegionName : null,
            LastUploadTimeUnixMilliseconds = lastUploadTimeUnixMilliseconds,
            StackSizeHistogram = Statistics.GetDistribution(salesHistory.Select(s => s.Quantity)),
            StackSizeHistogramNq = Statistics.GetDistribution(nqSales.Select(s => s.Quantity)),
            StackSizeHistogramHq = Statistics.GetDistribution(hqSales.Select(s => s.Quantity)),
            SaleVelocity = Statistics.VelocityPerDay(salesHistory.Select(s => s.TimestampUnixSeconds * 1000), now, statsWithin),
            SaleVelocityNq = Statistics.VelocityPerDay(nqSales.Select(s => s.TimestampUnixSeconds * 1000), now, statsWithin),
            SaleVelocityHq = Statistics.VelocityPerDay(hqSales.Select(s => s.TimestampUnixSeconds * 1000), now, statsWithin),
        };
    }
}