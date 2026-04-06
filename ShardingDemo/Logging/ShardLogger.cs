using ShardingDemo.Sharding;

namespace ShardingDemo.Logging;

/// <summary>
/// Color-coded console logger for the sharding demo.
/// Each shard has its own color; migrations are highlighted in yellow/red.
/// </summary>
public class ShardLogger
{
    private static readonly ConsoleColor[] ShardColors =
    [
        ConsoleColor.Cyan,
        ConsoleColor.Green,
        ConsoleColor.Magenta,
        ConsoleColor.Yellow,
        ConsoleColor.Blue
    ];

    // ── Structural ────────────────────────────────────────────────────────

    public void Section(string title)
    {
        Console.WriteLine();
        Write("━ ", ConsoleColor.DarkGray);
        Write(title.ToUpper(), ConsoleColor.Yellow, bold: true);
        Console.WriteLine();
        WriteDivider();
    }

    public void SubSection(string title)
    {
        Console.WriteLine();
        Write("  ▸ ", ConsoleColor.DarkYellow);
        Write(title, ConsoleColor.White);
        Console.WriteLine();
    }

    // ── Routing ───────────────────────────────────────────────────────────

    public void RouteDecision(string key, uint hash, int shardIndex, string operation)
    {
        Console.Write("  ");
        Write("[ROUTE]   ", ConsoleColor.DarkGray);
        Write($"key='{key}'", ConsoleColor.White);
        Console.Write("  →  ");
        Write($"hash=0x{hash:X8}", ConsoleColor.DarkCyan);
        Console.Write("  →  ring lookup  →  ");
        WriteShardLabel(shardIndex);
        Console.WriteLine();
        Console.Write("            ");
        Write($"OP: {operation}", ConsoleColor.DarkGray);
        Console.WriteLine();
    }

    public void FanOut(string message)
    {
        Console.Write("  ");
        Write("[FAN-OUT] ", ConsoleColor.DarkYellow);
        Write(message, ConsoleColor.White);
        Console.WriteLine();
    }

    public void ShardResult(int shardIndex, int count, string filter)
    {
        Console.Write("  ");
        Write("[GATHER]  ", ConsoleColor.DarkYellow);
        WriteShardLabel(shardIndex);
        Write($"  {count} row(s)  [{filter}]", ConsoleColor.Gray);
        Console.WriteLine();
    }

    public void FanOutComplete(int shardCount, int total)
    {
        Console.Write("  ");
        Write("[MERGE]   ", ConsoleColor.Blue);
        Write($"Merged {shardCount} shards → {total} total row(s)", ConsoleColor.White);
        Console.WriteLine();
    }

    public void Success(string message)
    {
        Console.Write("  ");
        Write("[OK]      ", ConsoleColor.DarkGreen);
        Write(message, ConsoleColor.Green);
        Console.WriteLine();
    }

    public void Info(string message)
    {
        Console.Write("  ");
        Write("[INFO]    ", ConsoleColor.DarkGray);
        Write(message, ConsoleColor.Gray);
        Console.WriteLine();
    }

    public void Highlight(string message)
    {
        Console.Write("  ");
        Write("★ ", ConsoleColor.Yellow);
        Write(message, ConsoleColor.White);
        Console.WriteLine();
    }

    // ── Migration ─────────────────────────────────────────────────────────

    public void MigrationStart(string message)
    {
        Console.WriteLine();
        Write("  ⚡ ", ConsoleColor.Yellow);
        Write(message, ConsoleColor.Yellow, bold: true);
        Console.WriteLine();
    }

    public void Migration(string customerId, string product, int fromShard, int toShard)
    {
        Console.Write("  ");
        Write("[MIGRATE] ", ConsoleColor.DarkYellow);
        Write($"'{customerId}' / {product,-22}", ConsoleColor.White);
        Console.Write("  ");
        WriteShardLabel(fromShard);
        Write("  →  ", ConsoleColor.DarkGray);
        WriteShardLabel(toShard);
        Console.WriteLine();
    }

    public void MigrationComplete(int migrated, int total, int shardIndex, string op)
    {
        Console.Write("  ");
        Write("[DONE]    ", ConsoleColor.Green);
        Write($"Shard {shardIndex} {op}. ", ConsoleColor.White);
        Write($"{migrated}/{total} order(s) migrated", ConsoleColor.Green);

        if (op == "added" && total > 0)
        {
            double pct = (double)migrated / total * 100;
            Write($"  ({pct:F1}% of data moved — consistent hashing advantage)", ConsoleColor.DarkGreen);
        }

        Console.WriteLine();
    }

    // ── Ring visualization ────────────────────────────────────────────────

    /// <summary>
    /// Prints a linear map of the uint32 token space divided into segments,
    /// showing which shard owns each segment.
    /// </summary>
    public void PrintRing(ConsistentHashRing ring, string label)
    {
        Console.WriteLine();
        Write($"  RING — {label}", ConsoleColor.Yellow, bold: true);
        Console.WriteLine();
        WriteDivider();

        Info($"Virtual nodes per shard : {ring.VirtualNodesPerShard}");
        Info($"Total tokens on ring    : {ring.TotalTokens}  ({ring.ActiveShards.Count} shards × {ring.VirtualNodesPerShard})");
        Console.WriteLine();

        // Token space ownership map (40 segments)
        var map = ring.GetOwnershipMap(40);
        Console.Write("  Token space [0x00000000 → 0xFFFFFFFF]  (40 segments)");
        Console.WriteLine();
        Console.Write("  ");
        foreach (var shard in map)
        {
            Write($"[S{shard}]", ShardColors[shard % ShardColors.Length]);
        }
        Console.WriteLine();

        // Ownership percentages
        Console.WriteLine();
        var pcts = ring.GetOwnershipPercents();
        Write("  Approx. token-space ownership:", ConsoleColor.Gray);
        Console.WriteLine();
        foreach (var (shard, pct) in pcts.OrderBy(x => x.Key))
        {
            Console.Write("  ");
            WriteShardLabel(shard);
            int bar = (int)Math.Round(pct / 100 * 30);
            Console.Write("  ");
            Write(new string('█', bar), ShardColors[shard % ShardColors.Length]);
            Write(new string('░', 30 - bar), ConsoleColor.DarkGray);
            Write($"  {pct:F1}%", ConsoleColor.Gray);
            Console.WriteLine();
        }

        WriteDivider();
    }

    // ── Distribution stats ────────────────────────────────────────────────

    public void PrintDistribution(Dictionary<int, int> distribution, int totalOrders)
    {
        Console.WriteLine();
        Write("  DATA DISTRIBUTION", ConsoleColor.Yellow, bold: true);
        Console.WriteLine();
        WriteDivider();

        int maxCount = distribution.Values.DefaultIfEmpty(0).Max();

        foreach (var (shard, count) in distribution.OrderBy(x => x.Key))
        {
            Console.Write("  ");
            WriteShardLabel(shard);
            Console.Write($"  {count,3} order(s)  ");

            int barLen = maxCount > 0 ? (int)Math.Round((double)count / maxCount * 30) : 0;
            Write(new string('█', barLen), ShardColors[shard % ShardColors.Length]);
            Write(new string('░', 30 - barLen), ConsoleColor.DarkGray);

            double pct = totalOrders > 0 ? (double)count / totalOrders * 100 : 0;
            Write($"  {pct,5:F1}%", ConsoleColor.Gray);
            Console.WriteLine();
        }

        WriteDivider();
        Write($"  TOTAL: {totalOrders} orders across {distribution.Count} shards", ConsoleColor.White);
        Console.WriteLine();
    }

    // ── Order table ───────────────────────────────────────────────────────

    public void PrintOrders(IEnumerable<Models.Order> orders, string label)
    {
        var list = orders.ToList();
        Console.WriteLine();
        Write($"  {label} ({list.Count} rows)", ConsoleColor.White);
        Console.WriteLine();

        if (list.Count == 0)
        {
            Write("  (no results)\n", ConsoleColor.DarkGray);
            return;
        }

        Write($"  {"#",-4} {"Customer",-14} {"Product",-24} {"Amount",8} {"Region",-12}", ConsoleColor.DarkGray);
        Console.WriteLine();
        Write("  " + new string('-', 66), ConsoleColor.DarkGray);
        Console.WriteLine();

        foreach (var o in list)
        {
            Console.Write("  ");
            Write($"{o.Id,-4} ", ConsoleColor.DarkGray);
            Write($"{o.CustomerName,-14} ", ConsoleColor.White);
            Write($"{o.Product,-24} ", ConsoleColor.Gray);
            Write($"${o.Amount,7:F2} ", ConsoleColor.Green);
            Write($"{o.Region,-12}", ConsoleColor.DarkCyan);
            Console.WriteLine();
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private void WriteShardLabel(int index)
    {
        Write($"[SHARD-{index}]", ShardColors[index % ShardColors.Length], bold: true);
    }

    private static void Write(string text, ConsoleColor color, bool bold = false)
    {
        Console.ForegroundColor = color;
        Console.Write(text);
        Console.ResetColor();
    }

    private static void WriteDivider()
    {
        Console.ForegroundColor = ConsoleColor.DarkGray;
        Console.WriteLine("  " + new string('─', 70));
        Console.ResetColor();
    }
}
