using Cassandra;
using Cassandra.Mapping;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using System;
using System.Linq;
using System.Net;
using Universalis.DbAccess.AccessControl;
using Universalis.DbAccess.MarketBoard;
using Universalis.DbAccess.Uploads;

namespace Universalis.DbAccess;

public static class DbAccessExtensions
{
    static DbAccessExtensions()
    {
        MappingConfiguration.Global.Define<ObjectMappings>();
    }

    public static void AddDbAccessServices(this IServiceCollection sc, IConfiguration configuration)
    {
        var redisCacheConnectionString = Environment.GetEnvironmentVariable("UNIVERSALIS_REDIS_CACHE_CONNECTION") ??
                                         configuration["RedisCacheConnectionString"] ??
                                         throw new InvalidOperationException(
                                             "Redis cache connection string not provided.");
        var redisConnectionString = Environment.GetEnvironmentVariable("UNIVERSALIS_REDIS_CONNECTION") ??
                                    configuration["RedisConnectionString"] ??
                                    throw new InvalidOperationException(
                                        "Redis primary connection string not provided.");
        var scyllaConnectionString = Environment.GetEnvironmentVariable("UNIVERSALIS_SCYLLA_CONNECTION") ??
                                     configuration["ScyllaConnectionString"] ??
                                     throw new InvalidOperationException(
                                         "ScyllaDB cache connection string not provided.");

        // Hack to avoid manually parsing hostname:port / ip:port combos
        var contactPoints = scyllaConnectionString.Split(',').SelectMany(s =>
        {
            var uri = new Uri("http://" + s);
            var port = uri.IsDefaultPort ? 9042 : uri.Port;
            var hostEntry = Dns.GetHostEntry(uri.DnsSafeHost);
            return hostEntry.AddressList.Select(addr => new IPEndPoint(addr, port));
        });
        var scyllaCluster = Cluster.Builder()
            .AddContactPoints(contactPoints)
            .Build();
        sc.AddSingleton<ICluster>(scyllaCluster);

        var cacheOptions = ConfigurationOptions.Parse(redisCacheConnectionString);
        var cache = Enumerable.Range(0, 3)
            .Select(_ => ConnectionMultiplexer.Connect(cacheOptions))
            .ToArray<IConnectionMultiplexer>();
        var dbOptions = ConfigurationOptions.Parse(redisConnectionString);
        var db = Enumerable.Range(0, 10)
            .Select(_ => ConnectionMultiplexer.Connect(dbOptions))
            .ToArray<IConnectionMultiplexer>();
        sc.AddSingleton<ICacheRedisMultiplexer>(_ => new WrappedRedisMultiplexer(cache));
        sc.AddSingleton<IPersistentRedisMultiplexer>(_ => new WrappedRedisMultiplexer(db));

        sc.AddSingleton<IWorldItemUploadStore, WorldItemUploadStore>();
        sc.AddSingleton<IMostRecentlyUpdatedDbAccess, MostRecentlyUpdatedDbAccess>();

        sc.AddSingleton<ICurrentlyShownStore, CurrentlyShownStore>();
        sc.AddSingleton<ICurrentlyShownDbAccess, CurrentlyShownDbAccess>();

        sc.AddSingleton<IMarketItemStore, MarketItemStore>();
        sc.AddSingleton<ISaleStore, SaleStore>();
        sc.AddSingleton<IHistoryDbAccess, HistoryDbAccess>();

        sc.AddSingleton<ISaleStatisticsDbAccess, SaleStatisticsDbAccess>();

        sc.AddSingleton<ICharacterStore, CharacterStore>();
        sc.AddSingleton<ICharacterDbAccess, CharacterDbAccess>();

        sc.AddSingleton<IFlaggedUploaderStore, FlaggedUploaderStore>();
        sc.AddSingleton<IFlaggedUploaderDbAccess, FlaggedUploaderDbAccess>();

        sc.AddSingleton<ITaxRatesStore, TaxRatesStore>();
        sc.AddSingleton<ITaxRatesDbAccess, TaxRatesDbAccess>();

        sc.AddSingleton<IWorldUploadCountStore, WorldUploadCountStore>();
        sc.AddSingleton<IWorldUploadCountDbAccess, WorldUploadCountDbAccess>();

        sc.AddSingleton<IDailyUploadCountStore, DailyUploadCountStore>();
        sc.AddSingleton<IUploadCountHistoryDbAccess, UploadCountHistoryDbAccess>();

        sc.AddSingleton<IApiKeyStore, ApiKeyStore>();
        sc.AddSingleton<ISourceUploadCountStore, TrustedSourceUploadCountStore>();
        sc.AddSingleton<ITrustedSourceDbAccess, TrustedSourceDbAccess>();

        sc.AddSingleton<IRecentlyUpdatedItemsStore, RecentlyUpdatedItemsStore>();
        sc.AddSingleton<IRecentlyUpdatedItemsDbAccess, RecentlyUpdatedItemsDbAccess>();
    }
}