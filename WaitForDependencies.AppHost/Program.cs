using Microsoft.Extensions.Diagnostics.HealthChecks;

var builder = DistributedApplication.CreateBuilder(args);

var sql = builder.AddSqlServer("sql")
    .WithHealthCheck();

var db = sql.AddDatabase("db");

var rabbit = builder.AddRabbitMQ("rabbit")
                    .WithHealthCheck();

var console = builder.AddProject<Projects.ConsoleApp1>("console");

var api0 = builder.AddProject<Projects.WebApplication2>("api0")
    .WithHealthCheck();

builder.AddProject<Projects.WebApplication1>("api")
    .WithExternalHttpEndpoints()
    .WithReference(db)
    .WithReference(rabbit)
    .WaitFor(db)
    .WaitFor(rabbit)
    .WaitFor(api0)
    .WaitForCompletion(console);

var failToStart = builder.AddExecutable("failToStart", "DoesNotExist.exe", ".");
var failToStartDependency = builder.AddProject<Projects.ConsoleApp1>("failToStartDependency")
    .WaitFor(failToStart);
var failToStartNestedDependency = builder.AddProject<Projects.ConsoleApp1>("failToStartNestedDependency")
    .WaitFor(failToStartDependency);


var healthy = builder.AddProject<Projects.ConsoleApp1>("healthy")
    .WithAnnotation(new HealthCheckAnnotation((_, _) => Task.FromResult<IHealthCheck?>(new FixedHealthCheck(HealthStatus.Healthy))));
var healthyDependency = builder.AddProject<Projects.ConsoleApp1>("healthyDependency")
    .WaitFor(healthy);


var unhealthy = builder.AddProject<Projects.ConsoleApp1>("unhealthy")
    .WithAnnotation(new HealthCheckAnnotation((_, _) => Task.FromResult<IHealthCheck?>(new FixedHealthCheck(HealthStatus.Unhealthy))));
var unhealthyDependency = builder.AddProject<Projects.ConsoleApp1>("unhealthyDependency")
    .WaitFor(unhealthy);

var degraded = builder.AddProject<Projects.ConsoleApp1>("degraded")
    .WithAnnotation(new HealthCheckAnnotation((_, _) => Task.FromResult<IHealthCheck?>(new FixedHealthCheck(HealthStatus.Degraded))));
var degradedDependency = builder.AddProject<Projects.ConsoleApp1>("degradedDependency")
    .WaitFor(degraded);


var healthCheckFailure = builder.AddProject<Projects.ConsoleApp1>("healthCheckFailure")
    .WithAnnotation(new HealthCheckAnnotation((_, _) => Task.FromResult<IHealthCheck?>(new FixedHealthCheck(HealthStatus.Degraded))));
var healthCheckFailureDependency = builder.AddProject<Projects.ConsoleApp1>("healthCheckFailureDependency")
    .WaitFor(healthCheckFailure);


var migrationMigrator = builder.AddProject<Projects.ConsoleApp1>("migrator")
    .WaitFor(sql);
var migrationDb = sql.AddDatabase("migrationDb")
    .WaitFor(migrationMigrator);

var resourceWaitingOnSelf = builder.AddProject<Projects.ConsoleApp1>("resourceWaitingOnSelf");
resourceWaitingOnSelf.WaitFor(resourceWaitingOnSelf);

builder.Build().Run();


public class FixedHealthCheck(HealthStatus status) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        => Task.FromResult(new HealthCheckResult(status));
}

class ThrowingHealthCheck() : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        => throw new Exception("Simulated exception");
}
