namespace ShardingDemo.SharedKernel.Models;

public class Order
{
    public int Id { get; set; }
    public string CustomerId { get; set; } = "";
    public string CustomerName { get; set; } = "";
    public string Product { get; set; } = "";
    public decimal Amount { get; set; }
    public string Region { get; set; } = "";
    public DateTime CreatedAt { get; set; }
}
