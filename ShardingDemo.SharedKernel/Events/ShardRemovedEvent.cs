namespace ShardingDemo.SharedKernel.Events;

public record ShardRemovedEvent(
    int ShardIndex,
    int OrdersMigrated,
    int TotalShardsAfter,
    DateTime OccurredAt);
