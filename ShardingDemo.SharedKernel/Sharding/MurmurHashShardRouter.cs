using System.Text;
using HashDepot;

namespace ShardingDemo.SharedKernel.Sharding;

/// <summary>
/// Modulo-based router using MurmurHash3. Kept for reference/comparison.
/// For dynamic scaling use ConsistentHashRouter instead.
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
