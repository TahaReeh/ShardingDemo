using ShardingDemo.KurrentDb.Infrastructure;
using ShardingDemo.KurrentDb.Logging;
using ShardingDemo.SharedKernel.Events;
using ShardingDemo.SharedKernel.Models;
using ShardingDemo.SharedKernel.Sharding;

namespace ShardingDemo.KurrentDb.Sharding;

/// <summary>
/// Handles all sharding operations against multiple KurrentDB instances.
///
/// Each order is routed to the KurrentDB instance that owns its shard:
///   hash(customerId) → ring lookup → shardIndex → KurrentDB[shardIndex]
///
/// Stream naming:
///   orders-{customerId}   → per-customer events, written to owning shard only
///   shard-operations      → topology events, broadcast to ALL shards
///
/// On scale-out (AddShard):
///   Events for customers that now route to the new shard are read from their
///   old shard and re-appended to the new shard — physical data migration.
///
/// On scale-in (RemoveShard):
///   Events for customers on the removed shard are migrated to their new owners
///   before the shard is taken offline.
/// </summary>
public class OrderEventService
{
    private const string ShardOpsStream = "shard-operations";

    private readonly ConsistentHashRouter    _router;
    private readonly KurrentDbService        _db;
    private readonly EventLogger             _log;
    private readonly Func<int, string>       _connectionStringFactory;

    public OrderEventService(
        ConsistentHashRouter router,
        KurrentDbService db,
        EventLogger log,
        Func<int, string> connectionStringFactory)
    {
        _router                   = router;
        _db                       = db;
        _log                      = log;
        _connectionStringFactory  = connectionStringFactory;
    }

    // ── Orders ────────────────────────────────────────────────────────────

    /// <summary>
    /// Routes the order via the ring, then appends OrderCreatedEvent
    /// to the correct KurrentDB shard instance.
    /// </summary>
    public async Task AppendOrderAsync(Order order)
    {
        uint   hash       = _router.ComputeHash(order.CustomerId);
        int    shardIndex = _router.GetShardIndex(order.CustomerId);
        string stream     = CustomerStream(order.CustomerId);

        _log.RouteDecision(order.CustomerId, hash, shardIndex,
            $"APPEND OrderCreatedEvent → shard {shardIndex} / stream '{stream}'");

        var @event = new OrderCreatedEvent(
            order.CustomerId,
            order.CustomerName,
            order.Product,
            order.Amount,
            order.Region,
            shardIndex,
            DateTime.UtcNow);

        await _db.AppendAsync(shardIndex, stream, @event);
        _log.Success($"Appended to KurrentDB Shard {shardIndex} (port {ShardPort(shardIndex)}) — stream '{stream}'");
    }

    // ── Topology events (broadcast to all shards) ─────────────────────────

    private async Task BroadcastShardAddedAsync(int shardIndex, int virtualNodes)
    {
        var @event = new ShardAddedEvent(shardIndex, virtualNodes, _router.ShardCount, DateTime.UtcNow);
        await _db.BroadcastAsync(ShardOpsStream, @event);
        _log.Success($"Broadcast ShardAddedEvent → '{ShardOpsStream}' on all shards");
    }

    private async Task BroadcastShardRemovedAsync(int shardIndex, int ordersMigrated)
    {
        var @event = new ShardRemovedEvent(shardIndex, ordersMigrated, _router.ShardCount, DateTime.UtcNow);
        await _db.BroadcastAsync(ShardOpsStream, @event);
        _log.Success($"Broadcast ShardRemovedEvent → '{ShardOpsStream}' on all shards");
    }

    private async Task BroadcastOrderMigratedAsync(string customerId, string product, int fromShard, int toShard)
    {
        var @event = new OrderMigratedEvent(customerId, product, fromShard, toShard, DateTime.UtcNow);
        await _db.BroadcastAsync(ShardOpsStream, @event);
        _log.Migration(customerId, product, fromShard, toShard);
    }

    // ── Scale-out: Add shard ──────────────────────────────────────────────

    /// <summary>
    /// Adds a new KurrentDB shard to the cluster:
    ///   1. Registers the new KurrentDB client
    ///   2. Updates the consistent hash ring
    ///   3. Migrates events for customers that now route to the new shard
    ///      (reads from old shard, re-appends to new shard)
    ///   4. Broadcasts topology events to all shards
    /// </summary>
    public async Task AddShardAsync(int newShardIndex, IReadOnlyList<Order> existingOrders)
    {
        _log.MigrationStart($"SCALE OUT — Shard {newShardIndex} joining the ring  (port {ShardPort(newShardIndex)})");

        // 1. Snapshot current routing BEFORE ring changes
        var customerOldShard = existingOrders
            .Select(o => o.CustomerId)
            .Distinct()
            .ToDictionary(cid => cid, cid => _router.GetShardIndex(cid));

        // 2. Register the new KurrentDB client + update ring
        _db.AddShard(newShardIndex, _connectionStringFactory(newShardIndex));
        _router.AddShard(newShardIndex);

        _log.Info($"KurrentDB Shard {newShardIndex} client registered (port {ShardPort(newShardIndex)})");
        _log.Info($"Ring now has {_router.Ring.TotalTokens} tokens across {_router.ShardCount} shards");

        // 3. Broadcast ShardAdded to all shards FIRST (topology record)
        await BroadcastShardAddedAsync(newShardIndex, _router.Ring.VirtualNodesPerShard);

        // 4. Migrate events for customers now owned by the new shard
        var migrated = 0;
        foreach (var (customerId, oldShard) in customerOldShard)
        {
            int newShard = _router.GetShardIndex(customerId);
            if (newShard == newShardIndex) // this customer moved to new shard
            {
                await MigrateCustomerEventsAsync(customerId, oldShard, newShard);
                await BroadcastOrderMigratedAsync(customerId, "(stream)", oldShard, newShard);
                migrated++;
            }
        }

        _log.MigrationComplete(migrated, customerOldShard.Count, newShardIndex, "added");
    }

    // ── Scale-in: Remove shard ────────────────────────────────────────────

    /// <summary>
    /// Removes a KurrentDB shard from the cluster:
    ///   1. Migrates all events from the shard being removed
    ///      (reads from old shard, re-appends to new ring owners)
    ///   2. Updates the ring + broadcasts topology events
    ///   3. Removes the KurrentDB client (container stays running but unused)
    /// </summary>
    public async Task RemoveShardAsync(int shardIndex, IReadOnlyList<Order> existingOrders)
    {
        _log.MigrationStart($"SCALE IN — Shard {shardIndex} leaving the ring  (port {ShardPort(shardIndex)})");

        // Find all customers currently on this shard
        var customersOnShard = existingOrders
            .Select(o => o.CustomerId)
            .Distinct()
            .Where(cid => _router.GetShardIndex(cid) == shardIndex)
            .ToList();

        _log.Info($"{customersOnShard.Count} customer stream(s) need to migrate off Shard {shardIndex}");

        // Remove from ring so re-routing takes effect
        _router.RemoveShard(shardIndex);
        _log.Info($"Ring now has {_router.Ring.TotalTokens} tokens across {_router.ShardCount} shards");

        // Migrate each customer's events to their new shard owner
        foreach (var customerId in customersOnShard)
        {
            int toShard = _router.GetShardIndex(customerId);
            await MigrateCustomerEventsAsync(customerId, shardIndex, toShard);
            await BroadcastOrderMigratedAsync(customerId, "(stream)", shardIndex, toShard);
        }

        // Broadcast removal event, then deregister client
        await BroadcastShardRemovedAsync(shardIndex, customersOnShard.Count);
        _db.RemoveShard(shardIndex);

        _log.MigrationComplete(customersOnShard.Count, customersOnShard.Count, shardIndex, "removed");
    }

    // ── Reads ─────────────────────────────────────────────────────────────

    /// <summary>Routes to the correct shard and reads the customer's event stream.</summary>
    public async Task<List<(string EventType, string Json)>> ReadCustomerStreamAsync(string customerId)
    {
        int shardIndex = _router.GetShardIndex(customerId);
        return await _db.ReadStreamAsync(shardIndex, CustomerStream(customerId));
    }

    /// <summary>Reads shard-operations from shard 0 (all shards hold identical copies).</summary>
    public Task<List<(string EventType, string Json)>> ReadShardOpsStreamAsync()
        => _db.ReadStreamAsync(0, ShardOpsStream);

    // ── Private helpers ───────────────────────────────────────────────────

    /// <summary>
    /// Reads all events for a customer from the source shard and
    /// re-appends them verbatim to the destination shard.
    /// The source stream is left intact (KurrentDB streams are immutable).
    /// </summary>
    private async Task MigrateCustomerEventsAsync(string customerId, int fromShard, int toShard)
    {
        string stream = CustomerStream(customerId);

        _log.Info($"  Migrating '{stream}'  Shard {fromShard} (port {ShardPort(fromShard)}) → Shard {toShard} (port {ShardPort(toShard)})");

        var events = await _db.ReadStreamAsync(fromShard, stream);

        foreach (var (eventType, json) in events)
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(json);
            await _db.AppendRawAsync(toShard, stream, eventType, bytes);
        }

        _log.Info($"  Migrated {events.Count} event(s) for '{customerId}'");
    }

    private static string CustomerStream(string customerId) => $"orders-{customerId}";
    private static int    ShardPort(int shardIndex)         => 2113 + shardIndex;
}
