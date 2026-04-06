namespace ShardingDemo.Sharding;

public interface IShardRouter
{
    /// <summary>Returns the zero-based shard index for a given sharding key.</summary>
    int GetShardIndex(string key);

    /// <summary>Raw MurmurHash3 value before ring lookup — useful for logging.</summary>
    uint ComputeHash(string key);

    int ShardCount { get; }
    IReadOnlyCollection<int> ActiveShards { get; }
}
