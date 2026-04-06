namespace ShardingDemo.SharedKernel.Events;

public record ShardAddedEvent(
    int ShardIndex,
    int VirtualNodes,
    int TotalShardsAfter,
    DateTime OccurredAt);
