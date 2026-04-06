using System.Text;
using System.Text.Json;
using KurrentDB.Client;

namespace ShardingDemo.KurrentDb.Infrastructure;

/// <summary>
/// Thin wrapper around the KurrentDB (EventStoreDB) gRPC client.
/// Responsibilities:
///   - Append a single typed event to a named stream
///   - Read all events from a named stream
///   - Provide the raw client for health checks
///
/// Stream naming convention (decided by callers, not here):
///   orders-{customerId}   → per-customer order events
///   shard-operations      → cluster topology events (add/remove shard, migrations)
/// </summary>
public class KurrentDbService : IAsyncDisposable
{
    private readonly KurrentDBClient _client;

    public KurrentDbService(string connectionString)
    {
        var settings = KurrentDBClientSettings.Create(connectionString);
        _client = new KurrentDBClient(settings);
    }

    // ── Write ─────────────────────────────────────────────────────────────

    /// <summary>Appends one event to the given stream. Creates the stream if it does not exist.</summary>
    public async Task AppendAsync<T>(string streamName, T @event) where T : class
    {
        var typeName  = typeof(T).Name;
        var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(@event);
        var eventData = new EventData(Uuid.NewUuid(), typeName, jsonBytes);

        await _client.AppendToStreamAsync(streamName, StreamState.Any, [eventData]);
    }

    // ── Read ──────────────────────────────────────────────────────────────

    /// <summary>
    /// Reads all events from a stream from the beginning.
    /// Returns (EventType, JsonPayload) tuples so callers can deserialize as needed.
    /// </summary>
    public async Task<List<(string EventType, string Json)>> ReadStreamAsync(string streamName)
    {
        var result = new List<(string, string)>();

        try
        {
            var events = _client.ReadStreamAsync(Direction.Forwards, streamName, StreamPosition.Start);

            await foreach (var resolvedEvent in events)
            {
                var json = Encoding.UTF8.GetString(resolvedEvent.Event.Data.Span);
                result.Add((resolvedEvent.Event.EventType, json));
            }
        }
        catch (StreamNotFoundException)
        {
            // Stream doesn't exist yet — return empty list
        }

        return result;
    }

    /// <summary>Checks whether a stream exists and has at least one event.</summary>
    public async Task<bool> StreamExistsAsync(string streamName)
    {
        var events = _client.ReadStreamAsync(Direction.Forwards, streamName, StreamPosition.Start, maxCount: 1);
        return await events.ReadState != ReadState.StreamNotFound;
    }

    public async ValueTask DisposeAsync() => await _client.DisposeAsync();
}
