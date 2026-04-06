namespace ShardingDemo.SharedKernel.Events;

public record OrderCreatedEvent(
    string CustomerId,
    string CustomerName,
    string Product,
    decimal Amount,
    string Region,
    int RoutedToShard,
    DateTime OccurredAt);
