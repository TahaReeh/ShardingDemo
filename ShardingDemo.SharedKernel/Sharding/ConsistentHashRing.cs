using System.Text;
using HashDepot;

namespace ShardingDemo.SharedKernel.Sharding;

/// <summary>
/// A consistent hash ring backed by a sorted token list.
///
/// Each physical shard is represented by <see cref="VirtualNodesPerShard"/> virtual nodes
/// (tokens) spread across the uint32 space via MurmurHash3.
///
/// Lookup (O(log N)):
///   hash(key) → binary-search for next clockwise token → that token's shard owns the key
///
/// Add shard    → insert N new tokens; only keys whose next-clockwise token changes migrate.
/// Remove shard → delete N tokens; those keys wrap to the next surviving token.
/// </summary>
public class ConsistentHashRing
{
    private readonly SortedList<uint, int> _ring = new();
    private readonly HashSet<int> _activeShards = new();
    private readonly int _virtualNodes;
    private const uint Seed = 0;

    public int VirtualNodesPerShard => _virtualNodes;
    public IReadOnlySet<int> ActiveShards => _activeShards;
    public int TotalTokens => _ring.Count;

    public ConsistentHashRing(int virtualNodes = 150)
    {
        if (virtualNodes < 1) throw new ArgumentOutOfRangeException(nameof(virtualNodes));
        _virtualNodes = virtualNodes;
    }

    // ── Shard management ──────────────────────────────────────────────────

    public void AddShard(int shardIndex)
    {
        if (_activeShards.Contains(shardIndex))
            throw new InvalidOperationException($"Shard {shardIndex} is already on the ring.");

        for (int i = 0; i < _virtualNodes; i++)
        {
            uint token = ComputeVnodeToken(shardIndex, i);
            while (_ring.ContainsKey(token)) token++; // nudge on collision
            _ring[token] = shardIndex;
        }

        _activeShards.Add(shardIndex);
    }

    public void RemoveShard(int shardIndex)
    {
        if (!_activeShards.Contains(shardIndex))
            throw new InvalidOperationException($"Shard {shardIndex} is not on the ring.");

        foreach (var token in _ring.Where(kv => kv.Value == shardIndex).Select(kv => kv.Key).ToList())
            _ring.Remove(token);

        _activeShards.Remove(shardIndex);
    }

    // ── Routing ───────────────────────────────────────────────────────────

    public int GetShardForKey(string key)
    {
        if (_ring.Count == 0) throw new InvalidOperationException("Ring has no shards.");
        return GetShardForToken(ComputeKeyHash(key));
    }

    public int GetShardForToken(uint token)
    {
        int index = LowerBound(token);
        if (index == _ring.Count) index = 0; // wrap around
        return _ring.Values[index];
    }

    // ── Hash computation ──────────────────────────────────────────────────

    public uint ComputeKeyHash(string key)
    {
        var bytes = Encoding.UTF8.GetBytes(key);
        return MurmurHash3.Hash32(bytes, Seed);
    }

    private uint ComputeVnodeToken(int shardIndex, int vnodeId)
    {
        var bytes = Encoding.UTF8.GetBytes($"shard-{shardIndex}-vnode-{vnodeId}");
        return MurmurHash3.Hash32(bytes, Seed);
    }

    // ── Introspection ─────────────────────────────────────────────────────

    public IReadOnlyList<(uint Token, int ShardIndex)> GetSnapshot() =>
        _ring.Select(kv => (kv.Key, kv.Value)).ToList();

    public int[] GetOwnershipMap(int segments = 40)
    {
        if (_ring.Count == 0) return new int[segments];
        var map = new int[segments];
        ulong range = (ulong)uint.MaxValue + 1;
        for (int s = 0; s < segments; s++)
        {
            uint probe = (uint)((ulong)s * (range / (ulong)segments));
            map[s] = GetShardForToken(probe);
        }
        return map;
    }

    public Dictionary<int, double> GetOwnershipPercents(int resolution = 1000)
    {
        var counts = _activeShards.ToDictionary(s => s, _ => 0);
        ulong range = (ulong)uint.MaxValue + 1;
        for (int i = 0; i < resolution; i++)
        {
            uint token = (uint)((ulong)i * (range / (ulong)resolution));
            counts[GetShardForToken(token)]++;
        }
        return counts.ToDictionary(kv => kv.Key, kv => (double)kv.Value / resolution * 100);
    }

    // ── Binary search ─────────────────────────────────────────────────────

    private int LowerBound(uint token)
    {
        int lo = 0, hi = _ring.Count - 1, result = _ring.Count;
        while (lo <= hi)
        {
            int mid = (lo + hi) / 2;
            if (_ring.Keys[mid] >= token) { result = mid; hi = mid - 1; }
            else lo = mid + 1;
        }
        return result;
    }
}
