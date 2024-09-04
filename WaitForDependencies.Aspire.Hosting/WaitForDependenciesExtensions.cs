using System.Runtime.ExceptionServices;
using Aspire.Hosting.ApplicationModel;
using Aspire.Hosting.Eventing;
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

#pragma warning disable ASPIREEVENTING001 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.
    private class WaitForDependenciesRunningHook(DistributedApplicationExecutionContext executionContext,
        ResourceNotificationService resourceNotificationService,
        ResourceLoggerService loggerService,
        IDistributedApplicationEventing distributedApplicationEventing,
        ILogger<WaitForDependenciesRunningHook> logger) :
        IDistributedApplicationLifecycleHook,
        IAsyncDisposable
    {
        private static readonly ResourceStateSnapshot _waitingState = new("Waiting", KnownResourceStateStyles.Info);
        private readonly CancellationTokenSource _cts = new();

        private DistributedApplicationEventSubscription? _eventSubscription;

        public Task BeforeStartAsync(DistributedApplicationModel appModel, CancellationToken cancellationToken = default)
        {
            // We don't need to execute any of this logic in publish mode
            if (executionContext.IsPublishMode)
            {
                return Task.CompletedTask;
            }

            _eventSubscription = distributedApplicationEventing.Subscribe<BeforeResourceStartedEvent>(async (data, cancellationToken) =>
            {
                Console.WriteLine($"Before {data.Resource.Name}");
                var resource = data.Resource;
                var blockers = GetBlockers(resource, cancellationToken);

                if (blockers.Count == 0)
                {
                    return;
                }

                ResourceStateSnapshot? initialState = null;
                await resourceNotificationService.PublishUpdateAsync(resource, s =>
                {
                    initialState = s.State;
                    return s with { State = _waitingState };
                });

                await Task.WhenAll(blockers).WaitAsync(cancellationToken);
                await resourceNotificationService.PublishUpdateAsync(resource, s => s with { State = initialState });
            });


            return Task.CompletedTask;
        }


        private List<Task> GetBlockers(IResource resource, CancellationToken cancellationToken)
        {
            var waitTasks = new List<Task>();

            if (resource.TryGetAnnotationsOfType<WaitForAnnotation>(out var waitOnAnnotations))
            {
                // REVIEW: This logic does not handle cycles in the dependency graph (that would result in a deadlock)
                foreach (var waitOn in waitOnAnnotations)
                {
                    var dependency = waitOn.Resource;

                    // Don't wait for yourself
                    if (dependency == resource)
                    {
                        continue;
                    }

                    waitTasks.Add(waitOn switch
                    {
                        { States: { } states } => WaitForDependencyToBeInState(dependency, states),
                        { WaitUntilCompleted: true } => WaitForDependencyToTerminate(dependency),
                        _ => WaitforDependencyToBeReady(dependency)
                    });
                }
            }

            return waitTasks;

            async Task WaitforDependencyToBeReady(IResource dependency)
            {
                loggerService.GetLogger(resource).LogInformation("⌛ Waiting for {Resource} to be ready", dependency.Name);
                try
                {
                    await resourceNotificationService.WaitForResourceAsync(dependency.Name, KnownResourceStates.Running, cancellationToken);
                    await WaitForHealthCheck(dependency, cancellationToken);
                    loggerService.GetLogger(resource).LogInformation("✅ {Resource} is ready", dependency.Name);
                }
                catch (Exception)
                {
                    loggerService.GetLogger(resource).LogError("❌ Dependency {Resource} failed to become ready", dependency.Name);
                    throw;
                }
            }

            async Task WaitForDependencyToTerminate(IResource dependency)
            {
                loggerService.GetLogger(resource).LogInformation("⌛ Waiting for {Resource} to complete", dependency.Name);

                loggerService.GetLogger(resource).LogInformation("✅ {Resource} is complete", dependency.Name);

                try
                {
                    await resourceNotificationService.WaitForResourceTerminationAsync(dependency.Name, cancellationToken);
                    //TODO: Add back healthchecks
                    loggerService.GetLogger(resource).LogInformation("✅ {Resource} is complete", dependency.Name);
                }
                catch (Exception)
                {
                    loggerService.GetLogger(resource).LogError("❌ Dependency {Resource} failed to become ready", dependency.Name);
                    throw;
                }

            }

            async Task WaitForDependencyToBeInState(IResource dependency, IEnumerable<string> targetStates)
            {
                loggerService.GetLogger(resource).LogInformation("⌛Waiting for {Resource} to be in state {TargetStates}", dependency.Name, targetStates);
                try
                {
                    await resourceNotificationService.WaitForResourceAsync(resource.Name, targetStates, cancellationToken);
                    loggerService.GetLogger(resource).LogInformation("✅ {Resource} is ready", dependency.Name);
                }
                catch (Exception)
                {
                    loggerService.GetLogger(resource).LogError("❌ Dependency {Resource} failed to reach state {TargetStates}", dependency.Name, targetStates);
                    throw;
                }
            }

            async Task WaitForHealthCheck(IResource resource, CancellationToken cancellationToken)
            {
                HealthCheckAnnotation? healthCheckAnnotation = null;
                while (true)
                {
                    // If we find a health check annotation, break out of the loop
                    if (resource.TryGetLastAnnotation(out healthCheckAnnotation))
                    {
                        break;
                    }

                    // If the resource has a parent, walk up the tree
                    if (resource is IResourceWithParent parent)
                    {
                        resource = parent.Parent;
                    }
                    else
                    {
                        break;
                    }
                }

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
                        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                        _ = Task.Run(async () =>
                        {
                            await resourceNotificationService.WaitForResourceTerminationAsync(resource.Name, cancellationToken);
                            _cts.Cancel();
                        }, cancellationToken);

                        await pipeline.ExecuteAsync(operation, cts.Token).AsTask().WaitAsync(cts.Token);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Healthcheck for {Resource} failed", resource.Name);
                        loggerService.GetLogger(resource).LogError(ex, "Healthcheck failed");
                        throw;
                    }

                    logger.LogInformation("{Resource} is healthy", resource.Name);
                    loggerService.GetLogger(resource).LogInformation("✅ Healthcheck has passed");
                }

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
        }

        public ValueTask DisposeAsync()
        {
            //TODO: Stop event subscriber
            _cts.Cancel();
            if (_eventSubscription != null)
            {
                distributedApplicationEventing.Unsubscribe(_eventSubscription);
            }
            return default;
        }
    }
}
#pragma warning restore ASPIREEVENTING001 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.