namespace ShardingDemo.SharedKernel.Sharding;

public interface IShardRouter
{
    int GetShardIndex(string key);
    uint ComputeHash(string key);
    int ShardCount { get; }
    IReadOnlyCollection<int> ActiveShards { get; }
}
