using Microsoft.EntityFrameworkCore;
using ShardingDemo.Data;
using ShardingDemo.Logging;
using ShardingDemo.Models;

namespace ShardingDemo.Sharding;

public record MigrationReport(
    int ShardIndex,
    string Operation,
    int OrdersMigrated,
    IReadOnlyList<string> AffectedCustomers);

/// <summary>
/// Provides CRUD and dynamic scaling operations over the sharded cluster.
///
/// Sharding key: Order.CustomerId
///   → All orders for the same customer always land on the same shard.
///
/// Write / direct read : O(log N) ring lookup → 1 shard.
/// Fan-out read        : scatter to all shards in parallel → gather + merge.
/// Add shard           : only keys whose "next clockwise token" changed need to migrate (~1/N of data).
/// Remove shard        : all keys on the removed shard migrate to their new ring owner.
/// </summary>
public class OrderShardingService
{
    private readonly ConsistentHashRouter _router;
    private readonly ShardContextFactory _factory;
    private readonly ShardLogger _log;

    public OrderShardingService(ConsistentHashRouter router, ShardContextFactory factory, ShardLogger log)
    {
        _router = router;
        _factory = factory;
        _log = log;
    }

    // ── INSERT ────────────────────────────────────────────────────────────

    public async Task InsertOrderAsync(Order order)
    {
        uint hash = _router.ComputeHash(order.CustomerId);
        int shardIndex = _router.GetShardIndex(order.CustomerId);

        _log.RouteDecision(order.CustomerId, hash, shardIndex,
            $"INSERT → {order.CustomerName}  |  {order.Product}  |  ${order.Amount:F2}");

        await using var ctx = _factory.CreateContext(shardIndex);
        ctx.Orders.Add(order);
        await ctx.SaveChangesAsync();

        _log.Success($"Saved → Shard {shardIndex}  (Id = {order.Id})");
    }

    // ── DIRECT LOOKUP — O(log N) ring lookup, then 1 shard ───────────────

    public async Task<List<Order>> GetOrdersByCustomerAsync(string customerId)
    {
        uint hash = _router.ComputeHash(customerId);
        int shardIndex = _router.GetShardIndex(customerId);

        _log.RouteDecision(customerId, hash, shardIndex, $"DIRECT QUERY — customer '{customerId}'");

        await using var ctx = _factory.CreateContext(shardIndex);
        var orders = await ctx.Orders
            .Where(o => o.CustomerId == customerId)
            .OrderBy(o => o.CreatedAt)
            .ToListAsync();

        _log.Success($"Found {orders.Count} order(s) on Shard {shardIndex}");
        return orders;
    }

    // ── FAN-OUT QUERIES — scatter/gather ──────────────────────────────────

    public async Task<List<Order>> GetAllOrdersAsync()
    {
        _log.FanOut("FAN-OUT — all orders from all shards");

        var tasks = _router.ActiveShards.Select(async shard =>
        {
            await using var ctx = _factory.CreateContext(shard);
            var rows = await ctx.Orders.OrderBy(o => o.CreatedAt).ToListAsync();
            _log.ShardResult(shard, rows.Count, "all orders");
            return rows;
        });

        var all = (await Task.WhenAll(tasks)).SelectMany(r => r).OrderBy(o => o.CreatedAt).ToList();
        _log.FanOutComplete(_router.ShardCount, all.Count);
        return all;
    }

    public async Task<List<Order>> GetOrdersByAmountRangeAsync(decimal min, decimal max)
    {
        _log.FanOut($"FAN-OUT — amount ${min:F2}–${max:F2}");

        var tasks = _router.ActiveShards.Select(async shard =>
        {
            await using var ctx = _factory.CreateContext(shard);
            var rows = await ctx.Orders
                .Where(o => o.Amount >= min && o.Amount <= max)
                .OrderBy(o => o.Amount)
                .ToListAsync();
            _log.ShardResult(shard, rows.Count, $"${min:F2}–${max:F2}");
            return rows;
        });

        var all = (await Task.WhenAll(tasks)).SelectMany(r => r).OrderBy(o => o.Amount).ToList();
        _log.FanOutComplete(_router.ShardCount, all.Count);
        return all;
    }

    public async Task<List<Order>> GetOrdersByRegionAsync(string region)
    {
        _log.FanOut($"FAN-OUT — region '{region}'");

        var tasks = _router.ActiveShards.Select(async shard =>
        {
            await using var ctx = _factory.CreateContext(shard);
            var rows = await ctx.Orders
                .Where(o => o.Region == region)
                .OrderBy(o => o.CustomerName)
                .ToListAsync();
            _log.ShardResult(shard, rows.Count, $"region '{region}'");
            return rows;
        });

        var all = (await Task.WhenAll(tasks)).SelectMany(r => r).OrderBy(o => o.CustomerName).ToList();
        _log.FanOutComplete(_router.ShardCount, all.Count);
        return all;
    }

    // ── DYNAMIC SCALING ───────────────────────────────────────────────────

    /// <summary>
    /// Adds a new shard to the cluster and migrates the keys that now belong to it.
    ///
    /// With consistent hashing, only ~1/N of existing keys need to move
    /// (compared to ~(N-1)/N for modulo-based sharding).
    /// </summary>
    public async Task<MigrationReport> AddShardAsync(int newShardIndex)
    {
        _log.MigrationStart($"SCALE OUT — adding Shard {newShardIndex} to the ring");

        // 1. Snapshot current shard → orders mapping BEFORE ring changes
        var snapshot = await SnapshotAllShardsAsync();

        // 2. Register the new shard in factory + ring
        _factory.AddShard(newShardIndex);
        _router.AddShard(newShardIndex);

        _log.Info($"Shard {newShardIndex} joined the ring ({_router.Ring.VirtualNodesPerShard} virtual nodes added)");
        _log.Info($"Ring now has {_router.Ring.TotalTokens} total tokens across {_router.ShardCount} shards");

        // 3. Find orders whose new shard differs from their current shard
        var migrations = snapshot
            .Where(x => _router.GetShardIndex(x.order.CustomerId) != x.currentShard)
            .ToList();

        // 4. Execute migrations
        var affectedCustomers = new HashSet<string>();
        foreach (var (order, fromShard) in migrations)
        {
            int toShard = _router.GetShardIndex(order.CustomerId);
            await MoveOrderAsync(order, fromShard, toShard);
            affectedCustomers.Add(order.CustomerId);
            _log.Migration(order.CustomerId, order.Product, fromShard, toShard);
        }

        _log.MigrationComplete(migrations.Count, snapshot.Count, newShardIndex, "added");
        return new MigrationReport(newShardIndex, "ADD", migrations.Count, affectedCustomers.ToList());
    }

    /// <summary>
    /// Removes a shard from the cluster, migrating all its data to the new ring owners.
    /// Every order on the removed shard must move; orders on other shards are unaffected.
    /// </summary>
    public async Task<MigrationReport> RemoveShardAsync(int shardIndex)
    {
        _log.MigrationStart($"SCALE IN — removing Shard {shardIndex} from the ring");

        // 1. Snapshot only the shard being removed
        var ordersToMove = await GetOrdersFromShardAsync(shardIndex);

        // 2. Remove from ring (re-routes its keys to adjacent shards)
        _router.RemoveShard(shardIndex);
        _factory.RemoveShard(shardIndex);

        _log.Info($"Shard {shardIndex} left the ring — {ordersToMove.Count} order(s) need to migrate");
        _log.Info($"Ring now has {_router.Ring.TotalTokens} tokens across {_router.ShardCount} shards");

        // 3. Migrate every order that was on the removed shard
        var affectedCustomers = new HashSet<string>();
        foreach (var order in ordersToMove)
        {
            int toShard = _router.GetShardIndex(order.CustomerId);
            await InsertIntoShardAsync(order, toShard);
            affectedCustomers.Add(order.CustomerId);
            _log.Migration(order.CustomerId, order.Product, shardIndex, toShard);
        }

        _log.MigrationComplete(ordersToMove.Count, ordersToMove.Count, shardIndex, "removed");
        return new MigrationReport(shardIndex, "REMOVE", ordersToMove.Count, affectedCustomers.ToList());
    }

    // ── STATS ─────────────────────────────────────────────────────────────

    public async Task<Dictionary<int, int>> GetShardDistributionAsync()
    {
        var dist = new Dictionary<int, int>();
        foreach (var shard in _router.ActiveShards)
        {
            await using var ctx = _factory.CreateContext(shard);
            dist[shard] = await ctx.Orders.CountAsync();
        }
        return dist;
    }

    public int TotalOrderCount => _router.ActiveShards.Sum(s =>
    {
        using var ctx = _factory.CreateContext(s);
        return ctx.Orders.Count();
    });

    // ── Private helpers ───────────────────────────────────────────────────

    private async Task<List<(Order order, int currentShard)>> SnapshotAllShardsAsync()
    {
        var result = new List<(Order, int)>();
        // Snapshot called before the new shard is added to the ring, so ActiveShards = existing shards only
        foreach (var shard in _router.ActiveShards.ToList())
        {
            await using var ctx = _factory.CreateContext(shard);
            var rows = await ctx.Orders.AsNoTracking().ToListAsync();
            result.AddRange(rows.Select(o => (o, shard)));
        }
        return result;
    }

    private async Task<List<Order>> GetOrdersFromShardAsync(int shardIndex)
    {
        await using var ctx = _factory.CreateContext(shardIndex);
        return await ctx.Orders.AsNoTracking().ToListAsync();
    }

    private async Task MoveOrderAsync(Order order, int fromShard, int toShard)
    {
        // Delete from old shard
        await using var oldCtx = _factory.CreateContext(fromShard);
        var existing = await oldCtx.Orders.FindAsync(order.Id);
        if (existing != null)
        {
            oldCtx.Orders.Remove(existing);
            await oldCtx.SaveChangesAsync();
        }

        // Insert into new shard
        await InsertIntoShardAsync(order, toShard);
    }

    private async Task InsertIntoShardAsync(Order order, int toShard)
    {
        await using var newCtx = _factory.CreateContext(toShard);
        // Clone without Id so the target DB assigns its own key
        newCtx.Orders.Add(new Order
        {
            CustomerId   = order.CustomerId,
            CustomerName = order.CustomerName,
            Product      = order.Product,
            Amount       = order.Amount,
            Region       = order.Region,
            CreatedAt    = order.CreatedAt
        });
        await newCtx.SaveChangesAsync();
    }
}
