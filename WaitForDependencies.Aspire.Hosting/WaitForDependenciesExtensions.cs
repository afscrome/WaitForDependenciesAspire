using System.Collections.Frozen;
using System.Runtime.ExceptionServices;
using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Lifecycle;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;

namespace Aspire.Hosting;

public static class WaitForDependenciesExtensions
{
    /// <summary>
    /// Wait for a resource to be running before starting another resource.
    /// </summary>
    /// <typeparam name="T">The resource type.</typeparam>
    /// <param name="builder">The resource builder.</param>
    /// <param name="other">The resource to wait for.</param>
    public static IResourceBuilder<T> WaitFor<T>(this IResourceBuilder<T> builder, IResourceBuilder<IResource> other)
        where T : IResource
    {
        builder.ApplicationBuilder.AddWaitForDependencies();
        return builder.WithAnnotation(new WaitForAnnotation(other.Resource));
    }

    /// <summary>
    /// Wait for a resource to run to completion before starting another resource.
    /// </summary>
    /// <typeparam name="T">The resource type.</typeparam>
    /// <param name="builder">The resource builder.</param>
    /// <param name="other">The resource to wait for.</param>
    public static IResourceBuilder<T> WaitForCompletion<T>(this IResourceBuilder<T> builder, IResourceBuilder<IResource> other)
        where T : IResource
    {
        builder.ApplicationBuilder.AddWaitForDependencies();
        return builder.WithAnnotation(new WaitForAnnotation(other.Resource) { WaitUntilCompleted = true });
    }

    /// <summary>
    /// Adds a lifecycle hook that waits for all dependencies to be "running" before starting resources. If that resource
    /// has a health check, it will be executed before the resource is considered "running".
    /// </summary>
    /// <param name="builder">The <see cref="IDistributedApplicationBuilder"/>.</param>
    private static IDistributedApplicationBuilder AddWaitForDependencies(this IDistributedApplicationBuilder builder)
    {
        builder.Services.TryAddLifecycleHook<WaitForDependenciesRunningHook>();
        return builder;
    }

    private class WaitForAnnotation(IResource resource) : IResourceAnnotation
    {
        public IResource Resource { get; } = resource;

        public string[]? States { get; set; }

        public bool WaitUntilCompleted { get; set; }
    }

    private class WaitForDependenciesRunningHook(DistributedApplicationExecutionContext executionContext,
        ResourceNotificationService resourceNotificationService,
        ResourceLoggerService loggerService,
        ILogger<WaitForDependenciesRunningHook> logger) :
        IDistributedApplicationLifecycleHook,
        IAsyncDisposable
    {
        private readonly CancellationTokenSource _cts = new();

        public Task BeforeStartAsync(DistributedApplicationModel appModel, CancellationToken cancellationToken = default)
        {
            // We don't need to execute any of this logic in publish mode
            if (executionContext.IsPublishMode)
            {
                return Task.CompletedTask;
            }

            var progress = appModel.Resources.ToFrozenDictionary(x => x, x => new ResourceProgress(x, resourceNotificationService, cancellationToken));

            // For each resource, add an environment callback that waits for dependencies to be running
            foreach (var resource in appModel.Resources)
            {
                var resourceProgress = progress[resource];

                // Abuse the environment callback to wait for dependencies to be running
                resource.Annotations.Add(new EnvironmentCallbackAnnotation(async context =>
                {
                    await resourceProgress.DependenciesReady.Task;
                }));

                _ = TrackResourceDependencies(resourceProgress, cancellationToken);
                _ = TrackResourceHealth(resourceProgress, cancellationToken);
                _ = ReportResourceState(resourceProgress, cancellationToken);
            }

            return Task.CompletedTask;

            async Task TrackResourceDependencies(ResourceProgress resourceProgress, CancellationToken cancellationToken)
            {
                var resource = resourceProgress.Resource;
                var waitTasks = new List<Task>();

                if (resource is IResourceWithParent resourceWithParent)
                {
                    waitTasks.Add(WaitforDependencyToBeHealthy(resourceWithParent.Parent));
                }

                if (resource.TryGetAnnotationsOfType<WaitForAnnotation>(out var waitOnAnnotations))
                {
                    foreach (var waitOn in waitOnAnnotations)
                    {
                        var dependency = waitOn.Resource;
                        waitTasks.Add(waitOn switch
                        {
                            { States: { } states } => WaitForDependencyToBeInState(dependency, states),
                            { WaitUntilCompleted: true } => WaitForDependencyToComplete(dependency),
                            _ => WaitforDependencyToBeHealthy(dependency)
                        });
                    }
                }

                if (waitTasks.Count == 0)
                {
                    resourceProgress.DependenciesReady.SetResult();
                    return;
                }

                try
                {
                    await Task.WhenAll(waitTasks)
                            .WaitAsync(cancellationToken);
                }
                catch (Exception ex)
                {
                    resourceProgress.DependenciesReady.SetException(ex);
                }

                resourceProgress.DependenciesReady.SetResult();

                async Task WaitforDependencyToBeHealthy(IResource dependency)
                {
                    loggerService.GetLogger(resource).LogInformation("⌛ Waiting for {Resource} to be Healthy", dependency.Name);
                    try
                    {
                        await progress[dependency].Healthy.Task;
                        loggerService.GetLogger(resource).LogInformation("✅ {Resource} is ready", dependency.Name);
                    }
                    catch (Exception ex)
                    {
                        loggerService.GetLogger(resource).LogError(ex, "Dependency {Resource} failed to become Healthy", dependency.Name);
                        throw;
                    }
                }

                async Task WaitForDependencyToComplete(IResource dependency)
                {
                    loggerService.GetLogger(resource).LogInformation("⌛ Waiting for {Resource} to complete", dependency.Name);
                    //TODO: Can this fail?  What would failure measn?
                    await progress[dependency].Finished;
                    loggerService.GetLogger(resource).LogInformation("✅ {Resource} is complete", dependency.Name);
                }

                async Task WaitForDependencyToBeInState(IResource dependency, IEnumerable<string> targetStates)
                {
                    loggerService.GetLogger(resource).LogInformation("⌛Waiting for {Resource} to be in state {TargetStates}", dependency.Name, targetStates);
                    try
                    {
                        await resourceNotificationService.WaitForResourceAsync(resource.Name, targetStates);
                        loggerService.GetLogger(resource).LogInformation("✅ {Resource} is ready", dependency.Name);
                    }
                    catch (Exception ex)
                    {
                        loggerService.GetLogger(resource).LogError(ex, "Dependency {Resource} failed to reach state {TargetStates}", dependency.Name, targetStates);
                        throw;
                    }
                }

            }

            async Task TrackResourceHealth(ResourceProgress resourceProgress, CancellationToken cancellationToken)
            {
                var resource = resourceProgress.Resource;

                // DependenciesReady and Started state may occur in any order:
                // - Resources with EnvironmentCallbackAnnotation are guaranteed to go DependenciesReady --> Started
                // - Resources without this can have DependenciesReady and Started happen in any order
                var healthcheckBlockers = Task.WhenAll(
                        resourceProgress.DependenciesReady.Task,
                        resourceProgress.Started
                    )
                    .WaitAsync(cancellationToken); ;

                var firstCompletedTask = await Task.WhenAny(
                    resourceProgress.Finished,
                    healthcheckBlockers
                    );

                if (firstCompletedTask == resourceProgress.Finished)
                {
                    resourceProgress.Healthy.SetCanceled();
                    return;
                }

                resource.TryGetLastAnnotation<HealthCheckAnnotation>(out var healthCheckAnnotation);
                Func<CancellationToken, ValueTask>? operation = null;

                if (healthCheckAnnotation?.HealthCheckFactory is { } factory)
                {
                    var check = await factory(resource, cancellationToken);

                    if (check is not null)
                    {
                        var context = new HealthCheckContext()
                        {
                            Registration = new HealthCheckRegistration("", check, HealthStatus.Unhealthy, [])
                        };

                        operation = async (cancellationToken) =>
                        {
                            var result = await check.CheckHealthAsync(context, cancellationToken);

                            if (result.Exception is not null)
                            {
                                ExceptionDispatchInfo.Throw(result.Exception);
                            }

                            if (result.Status != HealthStatus.Healthy)
                            {
                                throw new Exception("Health check failed");
                            }
                        };
                    }
                }

                if (operation is not null)
                {
                    var pipeline = CreateResiliencyPipeline();

                    logger.LogInformation("Starting Healthcheck for {Resource}", resource.Name);
                    loggerService.GetLogger(resource).LogInformation("🩺 Starting Healthcheck");
                    try
                    {
                        await pipeline.ExecuteAsync(operation, cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Healthcheck for {Resource} failed", resource.Name);
                        loggerService.GetLogger(resource).LogError(ex, "Healthcheck failed");

                        resourceProgress.Healthy.TrySetException(ex);

                        return;
                    }
                    logger.LogInformation("{Resource} is healthy", resource.Name);
                    loggerService.GetLogger(resource).LogInformation("✅ Healthcheck has passed");
                }
                resourceProgress.Healthy.SetResult();

                static ResiliencePipeline CreateResiliencyPipeline()
                {
                    var retryUntilCancelled = new RetryStrategyOptions()
                    {
                        ShouldHandle = new PredicateBuilder().Handle<Exception>(),
                        BackoffType = DelayBackoffType.Exponential,
                        MaxRetryAttempts = 5,
                        UseJitter = true,
                        MaxDelay = TimeSpan.FromSeconds(30)
                    };

                    return new ResiliencePipelineBuilder().AddRetry(retryUntilCancelled).Build();
                }
            }

            async Task ReportResourceState(ResourceProgress resourceProgress, CancellationToken cancellationToken)
            {
                // For resources that go WaitForDependencies -> Started -> Healthy, aspire's default state works well
                // This ordering is guaranteed for resources with 
                // Resources that go Started --> WaitForDependencies --> Healthy don't work so well
                // For such resources, this task adds in a waiting state.
                var resource = resourceProgress.Resource;
                await resourceNotificationService.WaitForResourceAsync(resourceProgress.Resource.Name, KnownResourceStates.Running, cancellationToken);

                if (resourceProgress.DependenciesReady.Task.IsCompletedSuccessfully)
                {
                    return;
                }

                ResourceStateSnapshot? initialState = null;

                await resourceNotificationService.PublishUpdateAsync(resource, s =>
                {
                    initialState = s.State;
                    return s with
                    {
                        State = new("Waiting", KnownResourceStateStyles.Info)
                    };
                });

                try
                {
                    await resourceProgress.DependenciesReady.Task;
                }
                catch (Exception)
                {
                    await resourceNotificationService.PublishUpdateAsync(resource, s => s with
                    {
                        //TODO: Should we have a dedicated `DependenciesFailedToStart` state?
                        State = new(KnownResourceStates.FailedToStart, KnownResourceStateStyles.Info)
                    });
                    return;
                }

                if (initialState != null)
                {
                    await resourceNotificationService.PublishUpdateAsync(resource, s =>
                        s.State?.Text != "Waiting"
                         ? s
                         : s with
                         {
                             State = initialState
                         });
                }
            }
        }

        public ValueTask DisposeAsync()
        {
            _cts.Cancel();
            return default;
        }
    }
}

internal static class ResourceNotificationServiceExtensions
{
    private static readonly string[] _knownTerminalResourceStates = [
            KnownResourceStates.Exited,
            KnownResourceStates.FailedToStart,
            KnownResourceStates.Finished
        ];

    public static async Task WaitForResourceTerminationAsync(this ResourceNotificationService resourceNotificationService, string resourceName, CancellationToken cancellationToken = default)
    {
        //TODO: Get ApplicationStopping token
        //using var watchCts = CancellationTokenSource.CreateLinkedTokenSource(_applicationStopping, cancellationToken);
        //var watchToken = watchCts.Token;

        await foreach (var resourceEvent in resourceNotificationService.WatchAsync(cancellationToken).ConfigureAwait(false))
        {
            if (string.Equals(resourceName, resourceEvent.Resource.Name, StringComparison.OrdinalIgnoreCase)
                && IsKnownTerminalState(resourceEvent.Snapshot))
            {
                return;
            }
        }

        throw new OperationCanceledException($"The operation was cancelled before the resource terminated");

        // These states are terminal but we need a better way to detect that
        static bool IsKnownTerminalState(CustomResourceSnapshot snapshot) =>
            _knownTerminalResourceStates.Contains(snapshot.State?.Text, StringComparer.OrdinalIgnoreCase)
            || snapshot.ExitCode is not null;
    }
}

internal class ResourceProgress(IResource resource, ResourceNotificationService resourceNotificationService, CancellationToken cancellationToken)
{
    public IResource Resource => resource;

    public Task Started { get; } = resourceNotificationService.WaitForResourceAsync(resource.Name, KnownResourceStates.Starting, cancellationToken);
    public TaskCompletionSource DependenciesReady { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public TaskCompletionSource Healthy { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    public Task Finished { get; } = resourceNotificationService.WaitForResourceTerminationAsync(resource.Name, cancellationToken);

}
