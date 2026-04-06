using System.Text;
using HashDepot;

namespace ShardingDemo.Sharding;

/// <summary>
/// A consistent hash ring backed by a sorted token list.
///
/// Each physical shard is represented by <see cref="VirtualNodesPerShard"/> virtual nodes
/// (tokens) spread evenly across the uint32 space via MurmurHash3.
/// The more virtual nodes, the more uniform the distribution.
///
/// Lookup (O(log N)):
///   hash(key) → binary-search for next clockwise token → that token's shard
///
/// Add shard  → insert N new tokens; only keys whose next-clockwise token changes need to migrate.
/// Remove shard → delete N tokens; those keys wrap to the next surviving token.
/// </summary>
public class ConsistentHashRing
{
    // Token position (uint) → physical shard index
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
            // Collision: nudge token until slot is free
            while (_ring.ContainsKey(token)) token++; // if this happen, will it cause in very small range between two tokens?
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

    /// <summary>Find the next clockwise token on the ring for a given position.</summary>
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

    // ── Introspection (for logging/visualization) ─────────────────────────

    /// <summary>Ordered snapshot of all ring tokens.</summary>
    public IReadOnlyList<(uint Token, int ShardIndex)> GetSnapshot() =>
        _ring.Select(kv => (kv.Key, kv.Value)).ToList();

    /// <summary>
    /// Divides the full uint32 token space into <paramref name="segments"/> equal slices
    /// and returns which shard owns each slice — used for the ring visualization.
    /// </summary>
    public int[] GetOwnershipMap(int segments = 40)
    {
        if (_ring.Count == 0) return new int[segments];

        var map = new int[segments];
        ulong range = (ulong)uint.MaxValue + 1; // 2^32

        for (int s = 0; s < segments; s++)
        {
            uint probeToken = (uint)((ulong)s * (range / (ulong)segments));
            map[s] = GetShardForToken(probeToken);
        }

        return map;
    }

    /// <summary>Returns the approx fraction of the token space owned by each shard.</summary>
    public Dictionary<int, double> GetOwnershipPercents(int resolution = 1000)
    {
        var counts = _activeShards.ToDictionary(s => s, _ => 0);
        ulong range = (ulong)uint.MaxValue + 1;

        for (int i = 0; i < resolution; i++)
        {
            uint token = (uint)((ulong)i * (range / (ulong)resolution));
            int shard = GetShardForToken(token);
            counts[shard]++;
        }

        return counts.ToDictionary(kv => kv.Key, kv => (double)kv.Value / resolution * 100);
    }

    // ── Binary search ─────────────────────────────────────────────────────

    /// <summary>Returns the index of the first key in the sorted list that is >= token.</summary>
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
