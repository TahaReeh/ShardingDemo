using System.Text;
using HashDepot;

namespace ShardingDemo.Sharding;

/// <summary>
/// Routes a string key to a shard index using MurmurHash3 (32-bit, seed=0).
/// MurmurHash3 gives a uniform distribution that minimises hotspots across shards.
///
/// Algorithm:
///   1. Encode key as UTF-8 bytes
///   2. Compute MurmurHash3 32-bit hash (HashDepot library)
///   3. shardIndex = hash % shardCount
/// </summary>
public class MurmurHashShardRouter : IShardRouter
{
    private const uint Seed = 0;

    public int ShardCount { get; }
    public IReadOnlyCollection<int> ActiveShards => Enumerable.Range(0, ShardCount).ToList();

    public MurmurHashShardRouter(int shardCount = 3)
    {
        if (shardCount < 1) throw new ArgumentOutOfRangeException(nameof(shardCount));
        ShardCount = shardCount;
    }

    public uint ComputeHash(string key)
    {
        var bytes = Encoding.UTF8.GetBytes(key);
        return MurmurHash3.Hash32(bytes, Seed);
    }

    public int GetShardIndex(string key)
    {
        uint hash = ComputeHash(key);
        return (int)(hash % (uint)ShardCount);
    }
}