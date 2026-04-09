# Sharding Deep Dive

## 1. Sharding Strategies

Sharding splits data across multiple nodes so no single node holds everything. There are four main strategies:

### Range-Based Sharding
Data is partitioned by a value range (e.g., orders with ID 1–1000 go to Shard 0, 1001–2000 to Shard 1).

- Simple to understand and implement
- Supports efficient range queries (e.g., "all orders from January")
- Suffers from **hot spots**: if most traffic targets a popular range, one shard gets overloaded while others sit idle
- Adding a shard requires defining a new range boundary and possibly splitting an existing range

### Directory-Based Sharding
A lookup table (directory) maps each key to its shard (e.g., `customerId → shardIndex`).

- Maximum flexibility — any mapping is possible
- The directory becomes a **single point of failure and a bottleneck**: every request hits it first
- Expensive to maintain at scale; the directory itself needs to be distributed

### Geographic Sharding
Data is placed on the shard nearest to where it originates or is consumed (e.g., EU customers → EU shard, US customers → US shard).

- Reduces latency for region-local reads/writes
- Meets data residency requirements
- Regions are uneven in traffic, so shards are uneven in load
- Requires a separate strategy (often hash-based) within each region

### Hash-Based Sharding
A hash function is applied to the key, and the result determines which shard receives the data.

- **Uniform distribution**: a good hash function spreads keys evenly with no configuration
- No hot spots as long as keys are diverse
- No directory lookup needed — routing is pure computation
- The tradeoff: range queries require a fan-out to all shards

---

## 2. Why Hash-Based Sharding for This Demo

This demo routes orders by `customerId`. The goals are:

- Every customer's data lives on exactly one shard (no cross-shard joins for a single customer)
- Load is distributed as evenly as possible regardless of who the customers are
- Routing must be deterministic and fast — no external lookup

Range-based sharding would cause hot spots if a few customers generate most of the load. Directory-based sharding introduces a lookup bottleneck and a new infrastructure dependency. Geographic sharding does not apply here.

Hash-based sharding satisfies all three goals with a single fast computation per request and no external dependency.

---

## 3. MurmurHash3 vs Other Hash Functions

Not every hash function is suitable for sharding. The criteria are: **speed**, **uniform distribution**, and **consistency** (same input always produces same output across machines and restarts).

| Hash Function | Speed    | Distribution | Notes |
|---------------|----------|--------------|-------|
| MD5 / SHA-256 | Slow     | Excellent    | Designed for cryptographic security — overkill and too slow for routing |
| CRC32         | Fast     | Moderate     | Good for checksums, clusters on structured inputs |
| FNV-1a        | Fast     | Good         | Simple, no external dependency, but not as uniform as MurmurHash3 on long keys |
| xxHash3       | Fastest  | Excellent    | Newer, slightly faster than MurmurHash3, but less battle-tested in distributed systems |
| MurmurHash3   | Very fast | Excellent   | Industry standard for sharding, used by Cassandra, Redis, Elasticsearch |

**Why MurmurHash3:**

- Proven uniform distribution across the 32-bit token space with diverse inputs
- Non-cryptographic — optimized for throughput, not security
- Established reference in distributed systems literature
- Consistent output regardless of platform or runtime (given same seed)
- Available as a well-maintained NuGet package (`HashDepot`)

The seed value is fixed at `0`. The seed does not improve distribution — it only changes which specific tokens each key maps to. Changing it in production would reroute every key to a different shard, which is effectively the same as losing all data locality. Keep the seed constant for the lifetime of the system.

---

## 4. The Scaling Problem with Naive Hash Sharding

With modulo hashing (`hash % N`), adding or removing a shard changes `N`, which changes the output of the formula for almost every key.

**Example — 3 shards, then scaled to 4:**

| CustomerId | hash % 3 | hash % 4 |
|------------|----------|----------|
| C001       | Shard 0  | Shard 2  |
| C002       | Shard 1  | Shard 3  |
| C003       | Shard 2  | Shard 1  |
| C004       | Shard 0  | Shard 0  |

Three out of four customers need to move. At scale, this means **migrating nearly all data** every time you add or remove a node — an operation that is expensive, disruptive, and risky.

The same problem occurs when scaling in (removing a shard): all keys must be rehashed and most end up on different shards than before.

---

## 5. Consistent Hashing

Consistent hashing places both **shards** and **keys** on the same circular token ring (the 32-bit uint space, wrapping at `uint.MaxValue`).

Each shard claims positions on the ring. To route a key, hash it to get a token, then walk clockwise until you hit the first shard token. That shard owns the key.

**What changes when you add a shard:**

Only the keys that fall between the new shard and its predecessor are affected. All other keys stay where they are.

- Adding 1 shard to a 3-shard system: ~25% of keys move (only from the shard that loses the range)
- Removing 1 shard from a 4-shard system: only the keys on that shard move (to the next shard clockwise)

This is the minimum possible data movement for any rebalancing.

### Virtual Nodes

A single token per shard still produces uneven ownership because the gaps between tokens vary randomly. Virtual nodes solve this: each shard places many tokens (this demo uses 150) spread across the ring using deterministic sub-hashes (e.g., `MurmurHash3("shard-2-vnode-47")`).

With 150 virtual nodes per shard and 3 shards:
- 450 tokens total on the ring
- Each shard owns ~33% of the token space
- Any individual gap between tokens is small — ownership is smooth

When a shard is added, its virtual nodes are interleaved throughout the ring. Each existing shard loses a small portion of its range to the new shard.

### Binary Search Routing

The ring is stored in a `SortedList<uint, int>`. Routing a key requires:
1. Compute `MurmurHash3(key)` → 32-bit token
2. Binary search the sorted list for the first token ≥ key token (O(log N))
3. If none found, wrap around to index 0

For 3 shards × 150 virtual nodes = 450 entries, a binary search takes ~9 comparisons. This is negligible.

---

## 6. Consistent Hashing vs Other Scaling Approaches

| Approach | Data Moved on Scale-Out | Complexity | Hot Spots |
|---|---|---|---|
| Modulo hashing | ~(N-1)/N of all keys | Low | None |
| Range splitting | Only the split range | Medium | Yes, if range is hot |
| Directory reassignment | Only reassigned keys | High (directory maintenance) | Depends on mapping |
| **Consistent hashing** | **~1/N of all keys** | Medium | None |

Consistent hashing gives the best data-movement guarantee of any approach that avoids a central directory.

The only case where it loses is range queries: because keys are scattered around the ring, a query like "all orders from this month" requires a fan-out to all shards. This demo accepts that tradeoff because routing by `customerId` means a single customer's data is always on one shard, making per-customer queries fast.

---

## 7. Real-World Databases Using This Approach

| Database | Hash Function | Virtual Nodes | Notes |
|---|---|---|---|
| **Apache Cassandra** | MurmurHash3 | Yes (configurable, default 256) | Pioneered virtual nodes ("vnodes") in distributed databases |
| **Amazon DynamoDB** | Undisclosed (consistent hash) | Yes (internal) | Each partition owns a token range; partitions split and merge automatically |
| **Redis Cluster** | CRC16 | No (16384 fixed slots) | Fixed slot count replaces virtual nodes; slots are reassigned manually or via CLUSTER REBALANCE |
| **Riak** | SHA-1 (truncated) | Yes (configurable) | Uses a 160-bit ring partitioned into a fixed number of virtual nodes |
| **Elasticsearch** | MurmurHash3 | No (fixed shards) | Number of primary shards is fixed at index creation; routing is `hash % num_shards` |

Cassandra is the closest model to this demo: MurmurHash3, virtual nodes, token ring, and data migration when the ring changes. The main difference is that Cassandra's ring state is gossiped between nodes (the nodes know the ring). In this demo, the application owns the ring — the database nodes are passive storage.
