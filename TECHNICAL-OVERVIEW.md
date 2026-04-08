# Database Sharding Demo — Technical Overview

**Platform:** .NET 8  
**Date:** April 2026  
**Purpose:** Proof-of-concept demonstrating consistent hash-based database sharding with dynamic scaling

---

## 1. What Problem Does This Solve?

As data volumes grow, a single database node becomes a bottleneck. **Sharding** splits data across multiple independent database nodes (shards), each holding a subset of the data.

The core challenge is **routing**: given a piece of data, which shard does it belong to?

### The Naive Approach — Modulo Hashing

The simplest strategy is:

```
shardIndex = hash(key) % numberOfShards
```

This works until you need to scale. If you go from 3 shards to 4:

- Every key's `hash % 4` produces a different result than `hash % 3`
- ~75% of all data must physically move to a different shard
- During migration, the system is partially unavailable or inconsistent

This is not acceptable for production systems.

### The Solution — Consistent Hashing

Consistent hashing reduces the migration cost to approximately **1/N of data** when adding one shard to an N-shard cluster. Going from 3 to 4 shards moves only ~25% of data. All other data stays exactly where it is.

This is the approach used by Cassandra, DynamoDB, Redis Cluster, and Apache Kafka.

---

## 2. How Consistent Hashing Works

### The Hash Ring

The full 32-bit integer space (0 → 4,294,967,295) is treated as a circular ring. Every shard is placed on this ring at multiple positions called **virtual nodes** (vnodes). When routing a key:

1. Hash the key to a position on the ring using MurmurHash3
2. Find the next clockwise vnode from that position
3. That vnode's shard owns the key

```
Ring (simplified)

     0x00000000
          │
 0x1A3F…  ●  Shard-0 (vnode 14)
 0x2BC1…  ●  Shard-1 (vnode 88)
 0x31D4…  ●  Shard-2 (vnode 3)
 0x3A99…  ●  Shard-0 (vnode 43)   ← same shard, different position
 0x4F12…  ●  Shard-2 (vnode 71)
          │
     0xFFFFFFFF (wraps back to top)
```

### Why Virtual Nodes?

Without vnodes, each shard owns one large contiguous arc. Depending on where the hash function places the shard's single token, one shard could end up owning 60% of the ring and another only 15%.

With 150 vnodes per shard, each shard owns 150 small scattered arcs. These fragments average out, giving each shard approximately equal ownership of the token space regardless of the hash function's luck on any single point.

| Vnodes per shard | Typical variance |
|---|---|
| 1 | ±40% (very uneven) |
| 50 | ±8% |
| 150 | ±3% (this demo) |
| 500 | ±1% |

### Adding a Shard (Scale Out)

When a new shard joins the ring, it inserts 150 new tokens. Each new token steals the beginning of the arc from whatever shard previously owned that range. Only keys that fall into these stolen arcs need to migrate. Everything else stays put.

**Result:** ~1/N of all data migrates. For 3→4 shards: ~25%.

### Removing a Shard (Scale In)

When a shard leaves the ring, its 150 tokens are removed. Each affected key then resolves to the next surviving clockwise token, which belongs to an adjacent shard. Only that shard's data needs to move. All other shards are completely unaffected.

---

## 3. Hash Function — MurmurHash3

**Library:** HashDepot 2.0.0 (NuGet)  
**Algorithm:** MurmurHash3, 32-bit output, seed = 0  
**Author:** Austin Appleby (2011)

MurmurHash3 was chosen for the following reasons:

- **Avalanche effect:** changing one character in the key changes ~50% of output bits, ensuring unrelated keys produce wildly different hash values and spread naturally across the ring
- **Uniform distribution:** passes SMHasher — the industry-standard hash quality test suite — with near-perfect scores
- **Speed:** processes input at ~800 MB/s; effectively instant for short strings like customer IDs
- **Determinism:** with a fixed seed of 0, the same key always produces the same hash value across all application instances and restarts, which is essential for consistent routing

The seed is fixed at 0. A randomised seed would be appropriate only if the sharding key were user-supplied free-text (to prevent HashDoS attacks). Since `CustomerId` values are system-generated, a fixed seed is the correct choice.

---

## 4. Solution Structure

The solution contains three projects:

```
ShardingDemo.sln
├── ShardingDemo.SharedKernel   — shared domain models, events, and ring logic
├── ShardingDemo                — EF Core demo (in-memory / SQL Server)
└── ShardingDemo.KurrentDb      — KurrentDB event-sourcing demo
```

### SharedKernel

Contains everything that both demos share. Has no dependency on any database framework — only HashDepot.

| Component | Purpose |
|---|---|
| `Order.cs` | Domain entity — the data being sharded |
| `IShardRouter` | Interface: given a key, return a shard index |
| `ConsistentHashRing` | The ring itself: sorted token list, binary search lookup, add/remove shard |
| `ConsistentHashRouter` | Implements `IShardRouter` using the ring; exposes `AddShard` / `RemoveShard` |
| `MurmurHashShardRouter` | Legacy modulo router — kept for comparison only |
| Domain events | `OrderCreatedEvent`, `OrderMigratedEvent`, `ShardAddedEvent`, `ShardRemovedEvent` |

### ShardingDemo (EF Core)

Demonstrates the full CRUD sharding lifecycle against a real database. Defaults to EF Core in-memory databases for zero infrastructure setup. Can be switched to SQL Server by changing one flag.

**NuGet packages:**
- `Microsoft.EntityFrameworkCore.InMemory` 9.0.3
- `Microsoft.EntityFrameworkCore.SqlServer` 9.0.3
- `HashDepot` 2.0.0

**Switching backends:**
```csharp
// Program.cs — line 22
bool UseInMemory = true;   // ← change to false for SQL Server
```

When SQL Server is enabled, each shard maps to a separate database:
- `ShardingDemo_Shard0`
- `ShardingDemo_Shard1`
- `ShardingDemo_Shard2`
- etc.

### ShardingDemo.KurrentDb

Demonstrates the same consistent hashing logic with **physically sharded KurrentDB instances**. Each shard is an independent KurrentDB container. The consistent hash ring routes every write to the correct container. Events are physically migrated between containers during scale-out and scale-in operations.

**NuGet packages:**
- `KurrentDB.Client` 1.3.1

**No projections, no read models** — this demo is purely about the event append and physical routing side.

**Shard → KurrentDB instance mapping:**

| Shard | Container | Port | Used when |
|---|---|---|---|
| 0 | `kurrentdb-shard0` | 2113 | Always |
| 1 | `kurrentdb-shard1` | 2114 | Initial 3-shard cluster; decommissioned in Step 5 |
| 2 | `kurrentdb-shard2` | 2115 | Always |
| 3 | `kurrentdb-shard3` | 2116 | Joined during scale-out in Step 4 |

**Two stream types:**

| Stream | Lives on | Contains |
|---|---|---|
| `orders-{customerId}` | Owning shard only | One `OrderCreatedEvent` per order |
| `shard-operations` | **All shards** (broadcast) | `ShardAddedEvent`, `ShardRemovedEvent`, `OrderMigratedEvent` |

**Why broadcast `shard-operations` to every shard?**
The KurrentDB instances are passive storage — they have no knowledge of the ring, routing, or each other. The broadcast is purely for the **application layer**:
- If the application process restarts, it replays `shard-operations` from any available shard to reconstruct the ring in memory
- If multiple application instances are running, each can independently replay the stream to sync their in-memory ring state
- The stream provides a human-readable audit trail of every topology change

Broadcasting to all shards ensures the audit log survives even if one shard goes down — any surviving instance can serve the replay.

---

## 5. Sharding Key

**Key:** `Order.CustomerId`

All orders belonging to the same customer are always routed to the same shard. This guarantees that a query such as "get all orders for customer CUST-001" hits exactly one shard with no fan-out required.

The trade-off is that queries which do not specify a customer — such as "all orders in region EU-West" or "all orders between $100 and $500" — cannot be narrowed to a single shard and must use the fan-out pattern.

---

## 6. Query Patterns

### Direct Lookup — O(log N)

Used when the sharding key (CustomerId) is known.

```
hash(customerId) → ring binary search → 1 shard → query
```

One shard is hit. No other shards are involved regardless of cluster size.

### Fan-Out — Scatter / Gather

Used when the sharding key is not part of the query predicate.

```
For each active shard (in parallel):
    Execute query with filter
    Return partial results

Merge all partial results in memory
```

All shards are queried in parallel. The application merges and sorts the combined results. This is more expensive but unavoidable for non-key queries.

---

## 7. Demo Walkthrough — EF Core Version

The demo runs as an interactive console application with 8 steps, pausing between each for review.

| Step | Action | Key observation |
|---|---|---|
| 1 | Initialize ring with 3 shards | 450 tokens placed on the ring (3 × 150) |
| 2 | Routing preview for 6 customer keys | Each key hashed → ring lookup → shard assigned |
| 3 | Insert 12 orders (6 customers, 2 orders each) | Each order routed and saved to correct shard |
| 4 | Direct lookup for CUST-001 and CUST-003 | Exactly 1 shard queried per lookup |
| 5 | **Scale out: add Shard 3** (3→4) | ~25% of orders migrate; 75% untouched |
| 6 | Fan-out query across all 4 shards | Demonstrates scatter/gather after scaling |
| 7 | **Scale in: remove Shard 1** (4→3) | Only Shard 1's data moves; shards 0, 2, 3 unaffected |
| 8 | Final fan-out verification | All 12 orders still present after both scaling operations |

---

## 8. Demo Walkthrough — KurrentDB Version

| Step | Action | Key observation |
|---|---|---|
| 1 | Connect to all 3 shard instances and initialize ring | One `KurrentDBClient` per shard; connectivity verified before proceeding |
| 2 | Append 12 orders — physically routed | Each order written to the correct KurrentDB container based on ring lookup |
| 3 | Direct read by customer | Ring routes to 1 instance — no other containers involved |
| 4 | **Scale out: add Shard 3** (3→4) | `kurrentdb-shard3` joins; customer streams physically migrated between containers; `ShardAddedEvent` broadcast to all 4 shards |
| 5 | **Scale in: remove Shard 1** (4→3) | All streams on `kurrentdb-shard1` read and re-appended to new owners; `ShardRemovedEvent` broadcast; client deregistered |
| 6 | Read `shard-operations` from Shard 0 | Full topology audit log — all 4 instances hold identical copies |

---

## 9. Infrastructure

Four independent KurrentDB containers run locally via Docker Compose, one per shard:

```yaml
# docker-compose.yml (solution root)
services:
  kurrentdb-shard0:   ports: ["2113:2113"]   # Shard 0
  kurrentdb-shard1:   ports: ["2114:2113"]   # Shard 1
  kurrentdb-shard2:   ports: ["2115:2113"]   # Shard 2
  kurrentdb-shard3:   ports: ["2116:2113"]   # Shard 3 (scale-out target)
```

Each container has its own named volume so data persists independently across restarts. All containers expose a `/health/live` endpoint monitored by Docker's healthcheck.

**Start all shards:**
```bash
docker compose up -d
```

**Management UIs (browse each shard's streams independently):**
- Shard 0 → http://localhost:2113
- Shard 1 → http://localhost:2114
- Shard 2 → http://localhost:2115
- Shard 3 → http://localhost:2116

**Stop and wipe all data:**
```bash
docker compose down -v
```

---

## 10. Technology Choices — Summary

| Technology | Version | Role | Why chosen |
|---|---|---|---|
| .NET 8 | 8.0 | Platform | LTS, modern C# features, high performance |
| MurmurHash3 | — | Hash function | Excellent distribution, battle-tested in sharding systems, fixed seed for determinism |
| HashDepot | 2.0.0 | MurmurHash3 implementation | Lightweight, well-maintained, idiomatic .NET API |
| EF Core InMemory | 9.0.3 | Shard storage (demo) | Zero infrastructure for local development and testing |
| EF Core SqlServer | 9.0.3 | Shard storage (production path) | Drop-in switch from in-memory; same DbContext code |
| KurrentDB Client | 1.3.1 | Event store client | Official client for KurrentDB / EventStoreDB |
| KurrentDB 24.10.0 | — | Event store | Industry-standard event store; persistent, ordered, replayable streams |
| Docker Compose | — | Local infrastructure | 4 independent KurrentDB containers, one per shard, started with a single command |

---

## 11. Comparison: Modulo vs. Consistent Hashing

|  | Modulo (`hash % N`) | Consistent Hashing (this demo) |
|---|---|---|
| **3 → 4 shards** | ~75% of data must move | ~25% of data must move |
| **Remove 1 shard** | Rehash everything | Only that shard's data moves |
| **Implementation complexity** | Trivial | Moderate |
| **Production suitability** | Fixed cluster size only | Designed for dynamic scaling |
| **Used by** | Simple caching layers | Cassandra, DynamoDB, Redis Cluster, Kafka |

---

## 12. Production Considerations

This demo illustrates the concepts. A production implementation would additionally require:

- **Distributed coordination** — a cluster manager (e.g. Zookeeper, etcd, or Consul) to broadcast ring changes to all application nodes so every instance routes identically
- **Migration atomicity** — two-phase commit or a saga pattern to ensure data is not lost or duplicated during migration if a node fails mid-migration
- **Replication** — each shard replicated to N nodes for fault tolerance
- **Consistent ring state** — all application nodes must see the same ring topology at the same time to avoid split-brain routing
- **Monitoring** — shard size metrics to detect hotspots and trigger rebalancing proactively
