using ShardingDemo.KurrentDb.Infrastructure;
using ShardingDemo.KurrentDb.Logging;
using ShardingDemo.KurrentDb.Sharding;
using ShardingDemo.SharedKernel.Models;
using ShardingDemo.SharedKernel.Sharding;

// ═══════════════════════════════════════════════════════════════════════════
//  KURRENTDB SHARDING DEMO — Consistent Hashing + Event Appending
//
//  This project focuses solely on the event side:
//    • No EF Core, no read models, no projections
//    • Every operation (order created, shard added/removed, migration)
//      is recorded as an immutable event in KurrentDB
//
//  Two stream types:
//    orders-{customerId}   → per-customer event history
//    shard-operations      → full cluster topology audit log
//
//  Routing still uses ConsistentHashRing from SharedKernel.
//  The resolved shard index is embedded in each event as metadata.
//
//  Prerequisites:
//    From the solution root: docker compose up -d
//    UI: http://localhost:2113
// ═══════════════════════════════════════════════════════════════════════════

const string ConnectionString = "esdb://localhost:2113?tls=false";
const int    InitialShards    = 3;
const int    VirtualNodes     = 150;

Console.OutputEncoding = System.Text.Encoding.UTF8;
Console.Clear();
PrintBanner();

var log     = new EventLogger();
var ring    = new ConsistentHashRing(VirtualNodes);
var router  = new ConsistentHashRouter(ring);
await using var db = new KurrentDbService(ConnectionString);
var service = new OrderEventService(router, db, log);

// ── Verify KurrentDB connectivity ─────────────────────────────────────────
log.Section("Connecting to KurrentDB");
log.Info($"Connection : {ConnectionString}");
log.Info("Start KurrentDB from solution root: docker compose up -d");
Console.WriteLine();

try
{
    // Probe by attempting a read — if KurrentDB is down this throws
    await db.ReadStreamAsync("$all-probe");
    log.Success("KurrentDB is reachable");
}
catch (Exception ex)
{
    Console.ForegroundColor = ConsoleColor.Red;
    Console.WriteLine($"  [ERROR] Cannot reach KurrentDB: {ex.Message}");
    Console.WriteLine("  Start KurrentDB with Docker (see header comment) and retry.");
    Console.ResetColor();
    return;
}

// ═══════════════════════════════════════════════════════════════════════════
//  STEP 1 — Initialize ring with 3 shards
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Step 1 — Initialize Ring (3 shards, no DB write yet)");
log.Info($"Virtual nodes : {VirtualNodes} per shard  ({InitialShards} × {VirtualNodes} = {InitialShards * VirtualNodes} tokens)");
log.Info("Ring lives in memory only — shard topology is recorded via ShardAddedEvent when needed.");
Console.WriteLine();

for (int i = 0; i < InitialShards; i++)
{
    router.AddShard(i);
    log.Success($"Shard {i} joined the ring  (+{VirtualNodes} virtual nodes)");
}

log.PrintRing(ring, "initial — 3 shards");
Pause();

// ═══════════════════════════════════════════════════════════════════════════
//  STEP 2 — Append orders to KurrentDB
//           Each order → stream "orders-{customerId}"
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Step 2 — Appending Orders to KurrentDB");
log.Info("Each order is routed by CustomerId → shard index is embedded in the event.");
log.Info("Stream: orders-{customerId}  (one stream per customer)");
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
//  STEP 3 — Read a customer stream back from KurrentDB
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Step 3 — Read Customer Streams from KurrentDB");
log.Info("KurrentDB stores the full history per customer regardless of shard topology.");
Console.WriteLine();

foreach (var customerId in new[] { "CUST-001", "CUST-002" })
{
    log.SubSection($"Stream: orders-{customerId}");
    var events = await service.ReadCustomerStreamAsync(customerId);
    log.PrintStream($"orders-{customerId}", events);
}

Pause();

// ═══════════════════════════════════════════════════════════════════════════
//  STEP 4 — Scale out: Add Shard 3, record topology + migration events
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Step 4 — Scale Out: Add Shard 3  (3 → 4 shards)");
log.Info("Appends: ShardAddedEvent + OrderMigratedEvent for each re-routed customer.");
log.Info("All events land in the 'shard-operations' stream.");
Console.WriteLine();

log.PrintRing(ring, "BEFORE adding Shard 3");
Pause();

await service.SimulateAddShardAsync(3, orders);
Console.WriteLine();
log.PrintRing(ring, "AFTER adding Shard 3");
Pause();

// ═══════════════════════════════════════════════════════════════════════════
//  STEP 5 — Scale in: Remove Shard 1, record topology + migration events
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Step 5 — Scale In: Remove Shard 1  (4 → 3 shards)");
log.Info("Appends: OrderMigratedEvent per customer + ShardRemovedEvent.");
Console.WriteLine();

log.PrintRing(ring, "BEFORE removing Shard 1");
Pause();

await service.SimulateRemoveShardAsync(1, orders);
Console.WriteLine();
log.PrintRing(ring, "AFTER removing Shard 1");
Pause();

// ═══════════════════════════════════════════════════════════════════════════
//  STEP 6 — Read shard-operations stream: full cluster audit log
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Step 6 — Read 'shard-operations' Stream (full audit log)");
log.Info("Every topology change ever made is recorded here in order.");
log.Info("You can replay this stream to reconstruct the cluster's history at any point.");
Console.WriteLine();

var shardOps = await service.ReadShardOpsStreamAsync();
log.PrintStream("shard-operations", shardOps);
Pause();

// ═══════════════════════════════════════════════════════════════════════════
//  SUMMARY
// ═══════════════════════════════════════════════════════════════════════════

log.Section("Summary");
log.Info("What this project demonstrates:");
log.Info("  • KurrentDB as the event store — no SQL, no EF Core");
log.Info("  • One stream per customer   → full order history, shard-agnostic");
log.Info("  • shard-operations stream  → immutable cluster audit log");
log.Info("  • Consistent hash routing from SharedKernel — same logic, different storage");
Console.WriteLine();
log.Info("Check the KurrentDB UI at http://localhost:2113 to browse streams directly.");
log.Highlight("Compare with ShardingDemo project: same ring, different backend.");
Console.WriteLine();

// ── Helpers ────────────────────────────────────────────────────────────────

static void PrintBanner()
{
    Console.ForegroundColor = ConsoleColor.DarkCyan;
    Console.WriteLine("""
        ╔═══════════════════════════════════════════════════════════════╗
        ║    KURRENTDB SHARDING DEMO — Consistent Hashing + Events      ║
        ║    Event Appending Only  |  SharedKernel  |  .NET 8           ║
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
