using Microsoft.EntityFrameworkCore;
using ShardingDemo.Models;

namespace ShardingDemo.Data;

public class ShardDbContext : DbContext
{
    public DbSet<Order> Orders { get; set; } = null!;

    public ShardDbContext(DbContextOptions<ShardDbContext> options) : base(options) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Order>(entity =>
        {
            entity.HasKey(o => o.Id);
            entity.Property(o => o.CustomerId).IsRequired().HasMaxLength(50);
            entity.Property(o => o.CustomerName).IsRequired().HasMaxLength(100);
            entity.Property(o => o.Product).IsRequired().HasMaxLength(200);
            entity.Property(o => o.Amount).HasPrecision(18, 2);
            entity.Property(o => o.Region).HasMaxLength(50);
        });
    }
}
