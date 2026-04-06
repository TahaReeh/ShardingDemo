using ShardingDemo.KurrentDb.Infrastructure;
using ShardingDemo.KurrentDb.Logging;
using ShardingDemo.SharedKernel.Events;
using ShardingDemo.SharedKernel.Models;
using ShardingDemo.SharedKernel.Sharding;

namespace ShardingDemo.KurrentDb.Sharding;

/// <summary>
/// Handles all sharding operations by appending domain events to KurrentDB.
/// No read models, no projections — pure event append.
///
/// Two stream types:
///   orders-{customerId}  — one stream per customer, contains OrderCreatedEvent
///   shard-operations     — cluster-level stream, contains Shard* and OrderMigrated events
///
/// Routing still uses ConsistentHashRouter from SharedKernel.
/// The shard index is recorded inside the event as metadata, not enforced here.
/// </summary>
public class OrderEventService
{
    private const string ShardOpsStream = "shard-operations";

    private readonly ConsistentHashRouter _router;
    private readonly KurrentDbService _db;
    private readonly EventLogger _log;

    public OrderEventService(ConsistentHashRouter router, KurrentDbService db, EventLogger log)
    {
        _router = router;
        _db     = db;
        _log    = log;
    }

    // ── Orders ────────────────────────────────────────────────────────────

    /// <summary>
    /// Routes the order via the consistent hash ring, then appends an
    /// OrderCreatedEvent to the customer's personal stream.
    /// </summary>
    public async Task AppendOrderAsync(Order order)
    {
        uint hash       = _router.ComputeHash(order.CustomerId);
        int  shardIndex = _router.GetShardIndex(order.CustomerId);
        string stream   = CustomerStream(order.CustomerId);

        _log.RouteDecision(order.CustomerId, hash, shardIndex,
            $"APPEND OrderCreatedEvent → stream '{stream}'");

        var @event = new OrderCreatedEvent(
            order.CustomerId,
            order.CustomerName,
            order.Product,
            order.Amount,
            order.Region,
            shardIndex,
            DateTime.UtcNow);

        await _db.AppendAsync(stream, @event);
        _log.Success($"Appended OrderCreatedEvent to '{stream}'");
    }

    // ── Shard topology events ─────────────────────────────────────────────

    public async Task AppendShardAddedAsync(int shardIndex, int virtualNodes)
    {
        var @event = new ShardAddedEvent(
            shardIndex,
            virtualNodes,
            _router.ShardCount,
            DateTime.UtcNow);

        await _db.AppendAsync(ShardOpsStream, @event);
        _log.Success($"Appended ShardAddedEvent (Shard {shardIndex}) → '{ShardOpsStream}'");
    }

    public async Task AppendShardRemovedAsync(int shardIndex, int ordersMigrated)
    {
        var @event = new ShardRemovedEvent(
            shardIndex,
            ordersMigrated,
            _router.ShardCount,
            DateTime.UtcNow);

        await _db.AppendAsync(ShardOpsStream, @event);
        _log.Success($"Appended ShardRemovedEvent (Shard {shardIndex}) → '{ShardOpsStream}'");
    }

    public async Task AppendOrderMigratedAsync(string customerId, string product, int fromShard, int toShard)
    {
        var @event = new OrderMigratedEvent(customerId, product, fromShard, toShard, DateTime.UtcNow);
        await _db.AppendAsync(ShardOpsStream, @event);
        _log.Migration(customerId, product, fromShard, toShard);
    }

    // ── Shard scale-out simulation ────────────────────────────────────────

    /// <summary>
    /// Simulates adding a shard:
    ///   1. Records ShardAdded topology event
    ///   2. Re-routes all known customers — those that move get an OrderMigrated event
    ///   3. Updates the router
    /// </summary>
    public async Task SimulateAddShardAsync(int newShardIndex, IReadOnlyList<Order> existingOrders)
    {
        _log.MigrationStart($"SCALE OUT — Shard {newShardIndex} joining the ring");

        // Snapshot current routing BEFORE adding to ring
        var before = existingOrders
            .Select(o => (order: o, oldShard: _router.GetShardIndex(o.CustomerId)))
            .ToList();

        // Add to ring
        _router.AddShard(newShardIndex);
        await AppendShardAddedAsync(newShardIndex, _router.Ring.VirtualNodesPerShard);

        _log.Info($"Ring now has {_router.Ring.TotalTokens} tokens across {_router.ShardCount} shards");

        // Detect and record migrations
        var migrated = 0;
        var seen     = new HashSet<string>();

        foreach (var (order, oldShard) in before)
        {
            int newShard = _router.GetShardIndex(order.CustomerId);
            if (newShard != oldShard && seen.Add(order.CustomerId))
            {
                await AppendOrderMigratedAsync(order.CustomerId, order.Product, oldShard, newShard);
                migrated++;
            }
        }

        _log.MigrationComplete(migrated, existingOrders.Count, newShardIndex, "added");
    }

    /// <summary>
    /// Simulates removing a shard:
    ///   1. Removes from ring (re-routes affected keys)
    ///   2. Records ShardRemoved + OrderMigrated events for every affected order
    /// </summary>
    public async Task SimulateRemoveShardAsync(int shardIndex, IReadOnlyList<Order> existingOrders)
    {
        _log.MigrationStart($"SCALE IN — Shard {shardIndex} leaving the ring");

        // Find all orders that currently live on this shard
        var affectedOrders = existingOrders
            .Where(o => _router.GetShardIndex(o.CustomerId) == shardIndex)
            .ToList();

        // Remove from ring
        _router.RemoveShard(shardIndex);

        _log.Info($"Ring now has {_router.Ring.TotalTokens} tokens across {_router.ShardCount} shards");

        // Record migrations for each affected order
        var seen = new HashSet<string>();
        foreach (var order in affectedOrders)
        {
            int toShard = _router.GetShardIndex(order.CustomerId);
            if (seen.Add(order.CustomerId))
                await AppendOrderMigratedAsync(order.CustomerId, order.Product, shardIndex, toShard);
        }

        await AppendShardRemovedAsync(shardIndex, seen.Count);
        _log.MigrationComplete(seen.Count, affectedOrders.Count, shardIndex, "removed");
    }

    // ── Stream reads ──────────────────────────────────────────────────────

    public Task<List<(string EventType, string Json)>> ReadCustomerStreamAsync(string customerId)
        => _db.ReadStreamAsync(CustomerStream(customerId));

    public Task<List<(string EventType, string Json)>> ReadShardOpsStreamAsync()
        => _db.ReadStreamAsync(ShardOpsStream);

    private static string CustomerStream(string customerId) => $"orders-{customerId}";
}
