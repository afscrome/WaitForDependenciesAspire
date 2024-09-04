using Aspire.Hosting.ApplicationModel;

namespace Aspire.Hosting;

internal static class ResourceNotificationServiceExtensions
{
    private static readonly string[] _knownTerminalResourceStates = [
            KnownResourceStates.Exited,
            KnownResourceStates.FailedToStart,
            KnownResourceStates.Finished
        ];

    public static async Task WaitForResourceTerminationAsync(this ResourceNotificationService resourceNotificationService, string resourceName, CancellationToken cancellationToken = default)
    {
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
#pragma warning restore ASPIREEVENTING001 // Type is for evaluation purposes only and is subject to change or removal in future updates. Suppress this diagnostic to proceed.