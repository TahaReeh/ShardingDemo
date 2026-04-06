namespace ShardingDemo.Sharding;

/// <summary>
/// IShardRouter implementation backed by a ConsistentHashRing.
/// Exposes AddShard / RemoveShard for dynamic cluster changes.
/// </summary>
public class ConsistentHashRouter : IShardRouter
{
    public ConsistentHashRing Ring { get; }

    public ConsistentHashRouter(ConsistentHashRing ring)
    {
        Ring = ring;
    }

    public int ShardCount => Ring.ActiveShards.Count;
    public IReadOnlyCollection<int> ActiveShards => Ring.ActiveShards;

    public uint ComputeHash(string key) => Ring.ComputeKeyHash(key);
    public int GetShardIndex(string key) => Ring.GetShardForKey(key);

    public void AddShard(int shardIndex) => Ring.AddShard(shardIndex);
    public void RemoveShard(int shardIndex) => Ring.RemoveShard(shardIndex);
}
