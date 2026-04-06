using Microsoft.EntityFrameworkCore;

namespace ShardingDemo.Data;

/// <summary>
/// Creates and tracks DbContext instances per shard.
/// Dictionary-based so shards can be added or removed at runtime.
///
/// Switch UseInMemory to false and ensure connection strings are valid to use SQL Server.
/// </summary>
public class ShardContextFactory
{
    private readonly bool _useInMemory;

    // shardIndex → in-memory db name  OR  SQL connection string
    private readonly Dictionary<int, string> _configs = new();

    public IReadOnlyList<int> RegisteredShards => _configs.Keys.Order().ToList();

    public ShardContextFactory(bool useInMemory = true)
    {
        _useInMemory = useInMemory;
    }

    public void AddShard(int shardIndex)
    {
        if (_useInMemory)
            _configs[shardIndex] = $"Shard_{shardIndex}";
        else
            _configs[shardIndex] = $"Server=.;Database=ShardingDemo_Shard{shardIndex};Trusted_Connection=True;TrustServerCertificate=True;";
    }

    public void RemoveShard(int shardIndex) => _configs.Remove(shardIndex);

    public ShardDbContext CreateContext(int shardIndex)
    {
        if (!_configs.TryGetValue(shardIndex, out var config))
            throw new InvalidOperationException($"Shard {shardIndex} is not registered in the factory.");

        var builder = new DbContextOptionsBuilder<ShardDbContext>();

        if (_useInMemory)
            builder.UseInMemoryDatabase(config);
        else
            builder.UseSqlServer(config);

        return new ShardDbContext(builder.Options);
    }
}
