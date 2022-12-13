using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace Universalis.DbAccess.Tests;

public class DbFixture : IAsyncLifetime
{
    private readonly IDockerContainer _scylla;
    private readonly IDockerContainer _cache;
    private readonly IDockerContainer _redis;

    private readonly Lazy<IServiceProvider> _services;

    public IServiceProvider Services => _services.Value;

    public DbFixture()
    {
        _scylla = new TestcontainersBuilder<TestcontainersContainer>()
            .WithName(Guid.NewGuid().ToString("D"))
            .WithImage("scylladb/scylla:5.1")
            .WithExposedPort(9042)
            .WithPortBinding(9042, true)
            .WithCommand("--smp", "1", "--developer-mode", "1", "--overprovisioned", "1", "--memory", "512M", "--skip-wait-for-gossip-to-settle", "0")
            .WithCreateContainerParametersModifier(o =>
            {
                o.HostConfig.CPUCount = 1;
            })
            .WithWaitStrategy(Wait.ForUnixContainer().UntilCommandIsCompleted("[ $(nodetool statusbinary) = running ]"))
            .Build();

        _cache = new TestcontainersBuilder<RedisTestcontainer>()
            .WithDatabase(new RedisTestcontainerConfiguration("redis:7.0"))
            .Build();
        _redis = new TestcontainersBuilder<RedisTestcontainer>()
            .WithDatabase(new RedisTestcontainerConfiguration("redis:7.0"))
            .Build();

        _services = new Lazy<IServiceProvider>(CreateServiceProvider);
    }

    private IServiceProvider CreateServiceProvider()
    {
        var services = new ServiceCollection();
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile(
                 path: "appsettings.Testing.json",
                 optional: false,
                 reloadOnChange: true)
            .AddInMemoryCollection(new Dictionary<string, string>
            {
                { "RedisCacheConnectionString", $"{_cache.Hostname}:{_cache.GetMappedPublicPort(6379)}" },
                { "RedisConnectionString", $"{_redis.Hostname}:{_redis.GetMappedPublicPort(6379)}" },
                { "ScyllaConnectionString", $"{_scylla.Hostname}:{_scylla.GetMappedPublicPort(9042)}" },
            })
            .Build();
        services.AddLogging();
        services.AddSingleton<IConfiguration>(configuration);
        services.AddDbAccessServices(configuration);
        return services.BuildServiceProvider();
    }

    public async Task InitializeAsync()
    {
        await Task.WhenAll(
            _scylla.StartAsync(),
            _cache.StartAsync(),
            _redis.StartAsync()
           );
    }

    public async Task DisposeAsync()
    {
        await _redis.DisposeAsync();
        await _cache.DisposeAsync();
        await _scylla.DisposeAsync();
    }
}
