using System.Text;
using System.Text.Json;
using KurrentDB.Client;

namespace ShardingDemo.KurrentDb.Infrastructure;

/// <summary>
/// Manages one KurrentDBClient per shard — the physical layer of sharding.
/// Each shard maps to an independent KurrentDB instance (separate container).
///
/// Shard map (matches docker-compose.yml):
///   Shard 0 → esdb://localhost:2113
///   Shard 1 → esdb://localhost:2114
///   Shard 2 → esdb://localhost:2115
///   Shard 3 → esdb://localhost:2116  (added during scale-out)
///
/// All stream operations require a shardIndex so the correct client is used.
/// The shard-operations stream is broadcast to ALL active shards so every
/// node holds a complete copy of the cluster topology history.
/// </summary>
public class KurrentDbService : IAsyncDisposable
{
    private readonly Dictionary<int, KurrentDBClient> _clients = new();

    // ── Shard registration ────────────────────────────────────────────────

    public void AddShard(int shardIndex, string connectionString)
    {
        if (_clients.ContainsKey(shardIndex))
            throw new InvalidOperationException($"Shard {shardIndex} is already registered.");

        var settings = KurrentDBClientSettings.Create(connectionString);
        _clients[shardIndex] = new KurrentDBClient(settings);
    }

    public void RemoveShard(int shardIndex)
    {
        if (_clients.Remove(shardIndex, out var client))
            client.Dispose();
    }

    public IReadOnlyCollection<int> ActiveShards => _clients.Keys.Order().ToList();

    // ── Write ─────────────────────────────────────────────────────────────

    /// <summary>Appends one event to the given stream on a specific shard.</summary>
    public async Task AppendAsync<T>(int shardIndex, string streamName, T @event) where T : class
    {
        var client    = GetClient(shardIndex);
        var typeName  = typeof(T).Name;
        var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(@event);
        var eventData = new EventData(Uuid.NewUuid(), typeName, jsonBytes);

        await client.AppendToStreamAsync(streamName, StreamState.Any, [eventData]);
    }

    /// <summary>
    /// Broadcasts one event to ALL active shards.
    /// Used for shard-operations so every node has the full topology history.
    /// </summary>
    public async Task BroadcastAsync<T>(string streamName, T @event) where T : class
    {
        var tasks = _clients.Keys.Select(shard => AppendAsync(shard, streamName, @event));
        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Re-appends a raw (already serialized) event to a shard.
    /// Used during migration to move events between shards without re-serializing.
    /// </summary>
    public async Task AppendRawAsync(int shardIndex, string streamName, string eventType, byte[] jsonBytes)
    {
        var client    = GetClient(shardIndex);
        var eventData = new EventData(Uuid.NewUuid(), eventType, jsonBytes);
        await client.AppendToStreamAsync(streamName, StreamState.Any, [eventData]);
    }

    // ── Read ──────────────────────────────────────────────────────────────

    /// <summary>Reads all events from a stream on a specific shard.</summary>
    public async Task<List<(string EventType, string Json)>> ReadStreamAsync(int shardIndex, string streamName)
    {
        var client = GetClient(shardIndex);
        var result = new List<(string, string)>();

        try
        {
            var events = client.ReadStreamAsync(Direction.Forwards, streamName, StreamPosition.Start);
            await foreach (var e in events)
            {
                var json = Encoding.UTF8.GetString(e.Event.Data.Span);
                result.Add((e.Event.EventType, json));
            }
        }
        catch (StreamNotFoundException) { }

        return result;
    }

    // ── Health ────────────────────────────────────────────────────────────

    /// <summary>Probes each shard by attempting a read. Throws if any shard is unreachable.</summary>
    public async Task VerifyAllShardsAsync()
    {
        var tasks = _clients.Select(async kv =>
        {
            try
            {
                var result = kv.Value.ReadStreamAsync(Direction.Forwards, "$probe", StreamPosition.Start, maxCount: 1);
                await result.ReadState;
            }
            catch (StreamNotFoundException) { } // stream not found is fine — server is up
        });
        await Task.WhenAll(tasks);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private KurrentDBClient GetClient(int shardIndex)
    {
        if (!_clients.TryGetValue(shardIndex, out var client))
            throw new InvalidOperationException($"No KurrentDB client registered for shard {shardIndex}.");
        return client;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var client in _clients.Values)
            await client.DisposeAsync();
    }
}
