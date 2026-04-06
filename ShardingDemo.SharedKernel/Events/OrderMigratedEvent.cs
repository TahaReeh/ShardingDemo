namespace ShardingDemo.SharedKernel.Events;

public record OrderMigratedEvent(
    string CustomerId,
    string Product,
    int FromShard,
    int ToShard,
    DateTime OccurredAt);
