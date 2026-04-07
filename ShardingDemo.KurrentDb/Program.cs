using ShardingDemo.KurrentDb.Infrastructure;
using ShardingDemo.KurrentDb.Logging;
using ShardingDemo.KurrentDb.Sharding;
using ShardingDemo.SharedKernel.Models;
using ShardingDemo.SharedKernel.Sharding;

// ═══════════════════════════════════════════════════════════════════════════
//  KURRENTDB SHARDING DEMO — Consistent Hashing + Physical Event Sharding
//
//  Each shard is an independent KurrentDB instance (separate Docker container).
//  The consistent hash ring routes every write to the correct instance.
//
//  Shard → KurrentDB instance → port:
//    Shard 0 → kurrentdb-shard0 → http://localhost:2113
//    Shard 1 → kurrentdb-shard1 → http://localhost:2114
//    Shard 2 → kurrentdb-shard2 → http://localhost:2115
//    Shard 3 → kurrentdb-shard3 → http://localhost:2116  (scale-out target)
//
//  Stream naming:
//    orders-{customerId}   → per-customer events on the owning shard only
//    shard-operations      → topology events broadcast to ALL shards
//
//  On migration, events are physically read from the source shard and
//  re-appended to the destination shard. Source streams remain (immutable).
//
//  Prerequisites:
//    From the solution root: docker compose up -d
//    UIs: http://localhost:2113  2114  2115  2116
// ═══════════════════════════════════════════════════════════════════════════

const int InitialShards = 3;
const int VirtualNodes  = 150;

// Connection string factory: shard index → KurrentDB port
static string ConnectionString(int shardIndex) =>
    $"esdb://localhost:{2113 + shardIndex}?tls=false";

Console.OutputEncoding = System.Text.Encoding.UTF8;
Console.Clear();
PrintBanner();

var log     = new EventLogger();
var ring    = new ConsistentHashRing(VirtualNodes);
var router  = new ConsistentHashRouter(ring);
await using var db = new KurrentDbService();
var service = new OrderEventService(router, db, log, ConnectionString);

// ═══════════════════════════════════════════════════════════════════════════
//  STEP 1 — Connect to all shard instances and initialize the ring
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Step 1 — Connect to KurrentDB Shards and Initialize Ring");
log.Info($"Virtual nodes : {VirtualNodes} per shard  ({InitialShards} × {VirtualNodes} = {InitialShards * VirtualNodes} tokens)");
Console.WriteLine();

for (int i = 0; i < InitialShards; i++)
{
    db.AddShard(i, ConnectionString(i));
    router.AddShard(i);
    log.Success($"Shard {i} registered  →  KurrentDB port {2113 + i}  (+{VirtualNodes} virtual nodes)");
}

// Verify all shard instances are reachable
log.Info("Verifying connectivity to all shard instances…");
try
{
    await db.VerifyAllShardsAsync();
    log.Success("All shard instances are reachable");
}
catch (Exception ex)
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine($"  [ERROR] {ex.Message}");
    Console.WriteLine("  Run: docker compose up -d  from the solution root.");
    Console.ResetColor();
    return;
}

log.PrintRing(ring, "initial — 3 shards");
Pause();

// ═══════════════════════════════════════════════════════════════════════════
//  STEP 2 — Append orders — each routed to its owning KurrentDB instance
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Step 2 — Appending Orders (physically routed to correct shard)");
log.Info("Each order's stream is written to the KurrentDB instance that owns its shard.");
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
    await service.AppendOrderAsync(order);
    Console.WriteLine();
}

Pause();

// ═══════════════════════════════════════════════════════════════════════════
//  STEP 3 — Read customer stream (routes to the correct shard automatically)
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Step 3 — Direct Read (ring routes to the correct KurrentDB instance)");
log.Info("Reading a customer stream hashes the customerId → finds the owning shard → reads from that instance only.");
Console.WriteLine();

foreach (var customerId in new[] { "CUST-001", "CUST-002" })
{
    int shard = router.GetShardIndex(customerId);
    log.SubSection($"orders-{customerId}  →  Shard {shard}  (port {2113 + shard})");
    var events = await service.ReadCustomerStreamAsync(customerId);
    log.PrintStream($"orders-{customerId}", events);
}

Pause();

// ═══════════════════════════════════════════════════════════════════════════
//  STEP 4 — Scale out: Add Shard 3 (physical migration between instances)
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Step 4 — Scale Out: Add Shard 3  (3 → 4 shards)");
log.Info("kurrentdb-shard3 joins the cluster on port 2116.");
log.Info("Customer streams that now route to Shard 3 are physically migrated:");
log.Info("  events read from old KurrentDB instance → re-appended to new instance.");
log.Info("ShardAddedEvent + OrderMigratedEvent broadcast to ALL shard instances.");
Console.WriteLine();

log.PrintRing(ring, "BEFORE adding Shard 3");
Pause();

await service.AddShardAsync(3, orders);
Console.WriteLine();
log.PrintRing(ring, "AFTER adding Shard 3");
Pause();

// ═══════════════════════════════════════════════════════════════════════════
//  STEP 5 — Scale in: Remove Shard 1 (migrate off before decommission)
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Step 5 — Scale In: Remove Shard 1  (4 → 3 shards)");
log.Info("All streams on kurrentdb-shard1 (port 2114) are migrated to new ring owners.");
log.Info("Shards 0, 2, and 3 are completely unaffected.");
log.Info("ShardRemovedEvent broadcast to all remaining shards.");
Console.WriteLine();

log.PrintRing(ring, "BEFORE removing Shard 1");
Pause();

await service.RemoveShardAsync(1, orders);
Console.WriteLine();
log.PrintRing(ring, "AFTER removing Shard 1");
Pause();

// ═══════════════════════════════════════════════════════════════════════════
//  STEP 6 — Read shard-operations: full cluster audit log
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Step 6 — 'shard-operations' Stream (cluster audit log)");
log.Info("Broadcast to all shards — every instance holds the full topology history.");
log.Info("Reading from Shard 0 (all copies are identical).");
Console.WriteLine();

var shardOps = await service.ReadShardOpsStreamAsync();
log.PrintStream("shard-operations", shardOps);
Pause();

// ═══════════════════════════════════════════════════════════════════════════
//  SUMMARY
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Summary");
log.Info("Physical sharding across 4 KurrentDB instances:");
log.Info("  Write     : hash(customerId) → ring → correct KurrentDB instance");
log.Info("  Read      : same routing → 1 instance hit, no fan-out needed");
log.Info("  Scale-out : events physically migrated to new instance (~1/N of data)");
log.Info("  Scale-in  : all events on removed instance migrated before shutdown");
log.Info("  Audit log : shard-operations broadcast to every instance");
Console.WriteLine();
log.Info("Browse each shard's streams:");
for (int i = 0; i < InitialShards; i++)
    log.Info($"  Shard {i} → http://localhost:{2113 + i}");
log.Info($"  Shard 3 → http://localhost:2116  (joined during demo)");
Console.WriteLine();

// ── Helpers ────────────────────────────────────────────────────────────────

static void PrintBanner()
{
    Console.ForegroundColor = ConsoleColor.DarkCyan;
    Console.WriteLine("""
        ╔═══════════════════════════════════════════════════════════════╗
        ║   KURRENTDB SHARDING DEMO — Physical Sharding Across          ║
        ║   4 Independent KurrentDB Instances  |  .NET 8                ║
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
