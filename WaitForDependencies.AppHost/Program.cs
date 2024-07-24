var builder = DistributedApplication.CreateBuilder(args);

var sql = builder.AddSqlServer("sql")
    .WithHealthCheck();

var dbMigrator = builder.AddProject<Projects.ConsoleApp1>("dbMigrator")
    .WaitFor(sql);

var db = sql.AddDatabase("db")
    .WaitForCompletion(dbMigrator);

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
    .WaitForCompletion(console)
    ;

var failToStart = builder.AddExecutable("failToStart", "DoesNotExist.exe", ".");

builder.Build().Run();
