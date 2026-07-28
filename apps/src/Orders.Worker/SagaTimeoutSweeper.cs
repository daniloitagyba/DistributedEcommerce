using Microsoft.Extensions.Options;

namespace Orders.Worker;

/// <summary>
/// The explicit compensation half of Milestone 22's comparison: the
/// choreographed saga has nothing equivalent to this. If Payments.Service
/// never replies, an order there just stays "Created" forever with no
/// automatic detection - here, the orchestrator itself owns noticing and
/// acting on that.
/// </summary>
public sealed class SagaTimeoutSweeper(
    IOptions<SagaOrchestrationOptions> options,
    SagaOrchestrationTracker tracker,
    ILogger<SagaTimeoutSweeper> logger) : BackgroundService
{
    private readonly SagaOrchestrationOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var timeout = TimeSpan.FromSeconds(_options.TimeoutSeconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(_options.SweepIntervalMilliseconds, stoppingToken);

            foreach (var (orderId, saga) in tracker.SweepTimedOut(timeout, DateTimeOffset.UtcNow))
            {
                SagaOrchestratorLog.SagaTimedOut(logger, orderId, _options.TimeoutSeconds, saga.CorrelationId);
            }
        }
    }
}
