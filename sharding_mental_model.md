# Sharding Mental Model (Step-by-Step)

## 1. Why Do We Need Sharding?

A single database cannot handle large amounts of data or high traffic.

So we split data across multiple databases (**shards**) to improve performance and scale horizontally.

---

## 2. How Do We Decide Where Data Goes?

We must answer:

> "Given a key (e.g., customerId), which shard should store it?"

We choose **Hash-Based Sharding** because:

- Even distribution of data — no hot spots
- No central lookup needed — routing is pure computation
- Fast — one hash call per request

---

## 3. Hash Function

```
hash = MurmurHash3(key, seed=0)
```

We use **MurmurHash3** because:

- Very fast (non-cryptographic)
- Uniform distribution across the 32-bit token space
- Deterministic — same key always produces same hash, across all machines and restarts
- Industry standard: used by Cassandra, Redis, Elasticsearch

> The seed must stay constant for the lifetime of the system. Changing it reroutes every key to a different shard — equivalent to losing all data locality.

---

## 4. Naive Approach

```
shard = hash % N
```

### Example (N = 3)

| Key | Hash | hash % 3 | Shard |
|-----|------|----------|-------|
| C1  | 10   | 1        | 1     |
| C2  | 25   | 1        | 1     |
| C3  | 8    | 2        | 2     |

Works fine — until you need to add or remove a shard.

---

## 5. Problem When Scaling

When N changes from 3 to 4, the formula changes for almost every key:

| Key | hash % 3 | hash % 4 | Moved? |
|-----|----------|----------|--------|
| C1  | 1        | 2        | Yes    |
| C2  | 1        | 1        | No     |
| C3  | 2        | 0        | Yes    |

2 out of 3 keys moved. At real scale (~billions of keys), nearly all data must be migrated every time a shard is added or removed.

> C2 staying in place is the exception, not the rule.

---

## 6. Consistent Hashing

Place both **shards** and **keys** on the same circular token ring (0 → 2³² wrapping back to 0).

Each shard claims positions (tokens) on the ring. To route a key:
1. Hash the key → get a token
2. Walk clockwise until the first shard token
3. That shard owns the key

### Ring Visualization

```
0 ---[10: Shard A]--- [50: Shard B]--- [90: Shard C]--- (wrap → 0)
```

---

## 7. Routing Example

```
hash("C1") = 46

46 → walk clockwise → 50 → Shard B
```

---

## 8. Virtual Nodes

Without virtual nodes, each shard occupies one position. The gaps between positions vary randomly, so ownership is uneven — one shard might own 60% of the ring, another only 10%.

**Virtual nodes** solve this: each shard places many tokens (e.g., 150) spread across the ring using deterministic sub-hashes.

```
10(A)  20(B)  30(C)  40(A)  50(B)  60(C)  70(A)  80(B)  90(C)
```

With 150 virtual nodes per shard, each shard owns approximately equal portions of the ring, and the distribution remains smooth regardless of how many shards exist.

---

## 9. Full Example — Scaling Out

### Before Adding Shard D

```
10(A)  20(B)  30(C)  40(A)  50(B)  60(C)  70(A)  80(B)  90(C)
```

```
hash = 52  →  walk clockwise  →  60  →  Shard C
```

### After Adding Shard D (with virtual nodes at 25, 55, 85)

```
10(A)  20(B)  25(D)  30(C)  40(A)  50(B)  55(D)  60(C)  70(A)  80(B)  85(D)  90(C)
```

```
hash = 52  →  walk clockwise  →  55  →  Shard D
```

### What Moved?

Only keys that fell in the ranges now owned by D:

```
(20 → 25]   previously owned by C, now D
(50 → 55]   previously owned by C, now D
(80 → 85]   previously owned by C, now D
```

Everything else stays exactly where it was. When adding 1 shard to a 3-shard system, only ~25% of keys move — the minimum possible.

---

## Summary

| Step | Action |
|------|--------|
| 1    | Hash the key with MurmurHash3 |
| 2    | Find the token on the ring |
| 3    | Walk clockwise to the first shard token |
| 4    | Route to that shard |

- Scalable — adding/removing a shard moves ~1/N of keys
- Balanced — virtual nodes ensure even ownership
- Efficient — routing is O(log N) binary search, no network lookup
