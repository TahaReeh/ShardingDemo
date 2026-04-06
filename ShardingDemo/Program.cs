using ShardingDemo.Data;
using ShardingDemo.Logging;
using ShardingDemo.Models;
using ShardingDemo.Sharding;

// ═══════════════════════════════════════════════════════════════════════════
//  CONSISTENT HASHING DEMO — MurmurHash3 + Virtual Nodes + Dynamic Scaling
//
//  Why consistent hashing instead of modulo (hash % N)?
//
//  Modulo problem: adding a shard changes where ~(N-1)/N of all keys route.
//  Example — 3→4 shards: ~75% of data needs to move.
//
//  Consistent hashing solution:
//    • Place N×V virtual nodes (tokens) on a uint32 ring (V = virtual nodes/shard)
//    • A key maps to the next clockwise token → that token's shard owns the key
//    • Adding 1 shard  → only ~1/N of keys change owner (3→4: ~25% moves)
//    • Removing 1 shard → only that shard's keys migrate to adjacent ring owners
//
//  To switch to SQL Server: change UseInMemory = false below.
// ═══════════════════════════════════════════════════════════════════════════

const int InitialShards = 3;
const int VirtualNodes  = 150;  // per shard — more = better distribution
bool      UseInMemory   = true; // ← flip to false for SQL Server

Console.OutputEncoding = System.Text.Encoding.UTF8;
Console.Clear();
PrintBanner();

var log     = new ShardLogger();
var ring    = new ConsistentHashRing(VirtualNodes);
var router  = new ConsistentHashRouter(ring);
var factory = new ShardContextFactory(UseInMemory);
var service = new OrderShardingService(router, factory, log);

// ═══════════════════════════════════════════════════════════════════════════
//  STEP 1 — Initialize ring with 3 shards
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Step 1 — Initialize Ring with 3 Shards");
log.Info($"Backend       : {(UseInMemory ? "In-Memory (EF Core)" : "SQL Server")}");
log.Info($"Virtual nodes : {VirtualNodes} per shard  ({InitialShards} × {VirtualNodes} = {InitialShards * VirtualNodes} tokens on the ring)");
log.Info($"Hash function : MurmurHash3 (32-bit, seed=0)  via HashDepot");
Console.WriteLine();

for (int i = 0; i < InitialShards; i++)
{
    factory.AddShard(i);
    router.AddShard(i);

    if (!UseInMemory)
    {
        await using var ctx = factory.CreateContext(i);
        await ctx.Database.EnsureCreatedAsync();
    }

    log.Success($"Shard {i} joined the ring  (+{VirtualNodes} virtual nodes)");
}

log.PrintRing(ring, "initial state — 3 shards");
Pause();

// ═══════════════════════════════════════════════════════════════════════════
//  STEP 2 — Routing preview
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Step 2 — Routing Preview (key → hash → ring lookup → shard)");
log.Info("Unlike modulo, lookup finds the next clockwise token from hash(key) on the ring.");
Console.WriteLine();

string[] previewKeys = ["CUST-001", "CUST-002", "CUST-003", "CUST-004", "CUST-005", "CUST-006"];
foreach (var key in previewKeys)
{
    uint h = router.ComputeHash(key);
    int shard = router.GetShardIndex(key);
    log.RouteDecision(key, h, shard, "preview only");
}

Pause();

// ═══════════════════════════════════════════════════════════════════════════
//  STEP 3 — Insert orders
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Step 3 — Inserting Orders");
log.Info("Each order is routed by CustomerId via the consistent hash ring.");
Console.WriteLine();

var orders = new List<Order>
{
    new() { CustomerId = "CUST-001", CustomerName = "Alice Martin",  Product = "Laptop Pro 15",       Amount = 1_299.99m, Region = "EU-West",    CreatedAt = DateTime.UtcNow.AddDays(-10) },
    new() { CustomerId = "CUST-002", CustomerName = "Bob Chen",      Product = "Wireless Headphones", Amount =   249.50m, Region = "US-East",    CreatedAt = DateTime.UtcNow.AddDays(-9)  },
    new() { CustomerId = "CUST-003", CustomerName = "Clara Lopez",   Product = "Standing Desk",       Amount =   899.00m, Region = "US-West",    CreatedAt = DateTime.UtcNow.AddDays(-8)  },
    new() { CustomerId = "CUST-001", CustomerName = "Alice Martin",  Product = "USB-C Hub",           Amount =    49.99m, Region = "EU-West",    CreatedAt = DateTime.UtcNow.AddDays(-7)  },
    new() { CustomerId = "CUST-004", CustomerName = "David Kim",     Product = "Mechanical Keyboard", Amount =   175.00m, Region = "APAC",       CreatedAt = DateTime.UtcNow.AddDays(-6)  },
    new() { CustomerId = "CUST-005", CustomerName = "Emma Dupont",   Product = "4K Monitor",          Amount = 1_050.00m, Region = "EU-West",    CreatedAt = DateTime.UtcNow.AddDays(-5)  },
    new() { CustomerId = "CUST-002", CustomerName = "Bob Chen",      Product = "Webcam HD",           Amount =    89.99m, Region = "US-East",    CreatedAt = DateTime.UtcNow.AddDays(-4)  },
    new() { CustomerId = "CUST-006", CustomerName = "Frank Müller",  Product = "NAS Drive 8TB",       Amount =   430.00m, Region = "EU-Central", CreatedAt = DateTime.UtcNow.AddDays(-3)  },
    new() { CustomerId = "CUST-003", CustomerName = "Clara Lopez",   Product = "Ergonomic Chair",     Amount =   699.00m, Region = "US-West",    CreatedAt = DateTime.UtcNow.AddDays(-2)  },
    new() { CustomerId = "CUST-004", CustomerName = "David Kim",     Product = "Stream Deck",         Amount =   149.99m, Region = "APAC",       CreatedAt = DateTime.UtcNow.AddDays(-1)  },
    new() { CustomerId = "CUST-005", CustomerName = "Emma Dupont",   Product = "Laptop Stand",        Amount =    59.90m, Region = "EU-West",    CreatedAt = DateTime.UtcNow               },
    new() { CustomerId = "CUST-006", CustomerName = "Frank Müller",  Product = "Smart Switch 8-Port", Amount =    95.00m, Region = "EU-Central", CreatedAt = DateTime.UtcNow.AddHours(1)  },
};

foreach (var order in orders)
{
    await service.InsertOrderAsync(order);
    Console.WriteLine();
}

log.Section("Distribution after initial inserts");
var dist1 = await service.GetShardDistributionAsync();
log.PrintDistribution(dist1, orders.Count);
Pause();

// ═══════════════════════════════════════════════════════════════════════════
//  STEP 4 — Direct lookup (single shard)
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Step 4 — Direct Lookup (ring routes to exactly 1 shard)");
log.Info("Sharding key known → hash → ring lookup → 1 shard hit. No scan of other shards.");
Console.WriteLine();

foreach (var cid in new[] { "CUST-001", "CUST-003" })
{
    log.SubSection($"Orders for {cid}");
    var customerOrders = await service.GetOrdersByCustomerAsync(cid);
    log.PrintOrders(customerOrders, $"Results for {cid}");
    Console.WriteLine();
}

Pause();

// ═══════════════════════════════════════════════════════════════════════════
//  STEP 5 — SCALE OUT: Add Shard 3  (3 → 4 shards)
//
//  Consistent hashing: ~1/N  of keys must move  (≈25%)
//  Modulo hashing    : ~(N-1)/N of keys must move (≈75%)
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Step 5 — Scale Out: Add Shard 3  (3 shards → 4 shards)");
log.Info("Consistent hashing: only ~1/N ≈ 25% of keys need to migrate.");
log.Info("Modulo would require ~75% of all keys to move — that is the key advantage.");
Console.WriteLine();

log.PrintRing(ring, "BEFORE adding Shard 3");
Pause();

var addReport = await service.AddShardAsync(3);
Console.WriteLine();

log.PrintRing(ring, "AFTER adding Shard 3");

log.Section("Distribution after adding Shard 3");
var dist2 = await service.GetShardDistributionAsync();
log.PrintDistribution(dist2, orders.Count);

log.Highlight($"Migrated  : {addReport.OrdersMigrated} order(s)  ({(double)addReport.OrdersMigrated / orders.Count * 100:F1}% of data moved)");
log.Highlight($"Untouched : {orders.Count - addReport.OrdersMigrated} order(s) stayed on their original shard");
log.Highlight($"Customers re-routed: {string.Join(", ", addReport.AffectedCustomers)}");
Pause();

// ═══════════════════════════════════════════════════════════════════════════
//  STEP 6 — Fan-out query after scaling (now 4 shards)
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Step 6 — Fan-Out Query Across All 4 Shards");
log.Info("Fan-out now hits all 4 shards in parallel and merges results.");
Console.WriteLine();

var allOrders = await service.GetAllOrdersAsync();
log.PrintOrders(allOrders, "All orders (merged from 4 shards)");
Pause();

// ═══════════════════════════════════════════════════════════════════════════
//  STEP 7 — SCALE IN: Remove Shard 1  (4 → 3 shards)
//
//  Simulate decommissioning a node (hardware failure / cost reduction).
//  Only Shard 1's data moves. Shards 0, 2, 3 are completely unaffected.
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Step 7 — Scale In: Remove Shard 1  (4 shards → 3 shards)");
log.Info("Decommission scenario: only that shard's orders migrate to their new ring owners.");
log.Info("Shards 0, 2, and 3 are completely unaffected.");
Console.WriteLine();

log.PrintRing(ring, "BEFORE removing Shard 1");
Pause();

var removeReport = await service.RemoveShardAsync(1);
Console.WriteLine();

log.PrintRing(ring, "AFTER removing Shard 1");

log.Section("Distribution after removing Shard 1");
var dist3 = await service.GetShardDistributionAsync();
log.PrintDistribution(dist3, orders.Count);

log.Highlight($"Migrated  : {removeReport.OrdersMigrated} order(s) off the decommissioned Shard 1");
log.Highlight($"Customers re-routed: {string.Join(", ", removeReport.AffectedCustomers)}");
Pause();

// ═══════════════════════════════════════════════════════════════════════════
//  STEP 8 — Final fan-out to verify all data is intact
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Step 8 — Final Verification (fan-out across shards 0, 2, 3)");
log.Info("All 12 orders must still be present, just redistributed across 3 remaining shards.");
Console.WriteLine();

var finalOrders = await service.GetAllOrdersAsync();
log.PrintOrders(finalOrders, "Final order list");
Console.WriteLine();
log.Highlight($"Total orders: {finalOrders.Count} / 12 — {(finalOrders.Count == 12 ? "all accounted for" : "MISMATCH - check migration logic")}");
Pause();

// ═══════════════════════════════════════════════════════════════════════════
//  SUMMARY
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Summary");
log.Info("Consistent hashing vs modulo:");
Console.WriteLine();
log.Info("  MODULO (hash % N):");
log.Info("    Simple, but 3→4 shards forces ~75% of all data to move");
Console.WriteLine();
log.Info("  CONSISTENT HASHING (this demo):");
log.Info("    Ring with virtual nodes → only ~1/N of data moves on resize");
log.Info("    Remove a shard → only that shard's data migrates to ring neighbours");
log.Info("    Used by: Cassandra, DynamoDB, Redis Cluster, Kafka");
Console.WriteLine();
log.Info($"  Virtual nodes ({VirtualNodes}/shard): more nodes = better load balance, more memory");
log.Info("  Sharding key  : CustomerId — all orders per customer stay on same shard");
log.Info("  Switch backend: set UseInMemory = false at top of Program.cs");
Console.WriteLine();

// ── Helpers ────────────────────────────────────────────────────────────────

static void PrintBanner()
{
    Console.ForegroundColor = ConsoleColor.DarkCyan;
    Console.WriteLine("""
        ╔═══════════════════════════════════════════════════════════════╗
        ║    CONSISTENT HASHING DEMO — MurmurHash3 + Virtual Nodes      ║
        ║    Dynamic Scaling  |  In-Memory DB  |  EF Core  |  .NET 8    ║
        ╚═══════════════════════════════════════════════════════════════╝
        """);
    Console.ResetColor();
}

static void Pause()
{
    Console.ForegroundColor = ConsoleColor.DarkGray;
    Console.Write("\n  Press any key to continue…");
    Console.ResetColor();
    Console.ReadKey(intercept: true);
    Console.WriteLine();
}
