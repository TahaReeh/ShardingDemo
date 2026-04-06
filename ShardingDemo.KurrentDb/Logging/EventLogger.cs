using ShardingDemo.SharedKernel.Sharding;

namespace ShardingDemo.KurrentDb.Logging;

/// <summary>
/// Color-coded console logger for the KurrentDB event appending demo.
/// Shares the same visual style as ShardLogger in the main project.
/// </summary>
public class EventLogger
{
    private static readonly ConsoleColor[] ShardColors =
    [
        ConsoleColor.Cyan,
        ConsoleColor.Green,
        ConsoleColor.Magenta,
        ConsoleColor.Yellow,
        ConsoleColor.Blue
    ];

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
        Write($"Shard {shardIndex} {op}.  ", ConsoleColor.White);
        Write($"{migrated} customer(s) re-routed", ConsoleColor.Green);

        if (op == "added" && total > 0)
        {
            double pct = (double)migrated / total * 100;
            Write($"  ({pct:F1}% of keys moved)", ConsoleColor.DarkGreen);
        }
        Console.WriteLine();
    }

    public void PrintRing(ConsistentHashRing ring, string label)
    {
        Console.WriteLine();
        Write($"  RING — {label}", ConsoleColor.Yellow, bold: true);
        Console.WriteLine();
        WriteDivider();

        Info($"Virtual nodes per shard : {ring.VirtualNodesPerShard}");
        Info($"Total tokens on ring    : {ring.TotalTokens}");
        Console.WriteLine();

        var map = ring.GetOwnershipMap(40);
        Console.Write("  Token space [0x00000000 → 0xFFFFFFFF]  (40 segments)\n  ");
        foreach (var shard in map)
            Write($"[S{shard}]", ShardColors[shard % ShardColors.Length]);
        Console.WriteLine();

        Console.WriteLine();
        var pcts = ring.GetOwnershipPercents();
        Write("  Approx. token-space ownership:\n", ConsoleColor.Gray);
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

    /// <summary>Prints a stream's events as a numbered timeline.</summary>
    public void PrintStream(string streamName, List<(string EventType, string Json)> events)
    {
        Console.WriteLine();
        Write($"  STREAM: {streamName}", ConsoleColor.Yellow, bold: true);
        Write($"  ({events.Count} event(s))", ConsoleColor.Gray);
        Console.WriteLine();
        WriteDivider();

        if (events.Count == 0)
        {
            Write("  (empty stream)\n", ConsoleColor.DarkGray);
            return;
        }

        for (int i = 0; i < events.Count; i++)
        {
            var (type, json) = events[i];
            Console.Write("  ");
            Write($"#{i + 1,-3} ", ConsoleColor.DarkGray);
            Write($"{type,-26} ", ConsoleColor.Cyan);
            Write(TrimJson(json), ConsoleColor.Gray);
            Console.WriteLine();
        }

        WriteDivider();
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private void WriteShardLabel(int index) =>
        Write($"[SHARD-{index}]", ShardColors[index % ShardColors.Length], bold: true);

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

    /// <summary>Collapses the JSON to a single readable line capped at 80 chars.</summary>
    private static string TrimJson(string json)
    {
        var trimmed = json.Replace("\n", " ").Replace("  ", " ");
        return trimmed.Length > 80 ? trimmed[..80] + "…" : trimmed;
    }
}
