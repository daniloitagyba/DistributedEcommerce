using Microsoft.Extensions.Options;

namespace Orders.Worker;

/// <summary>
/// The explicit compensation half of Milestone 22's comparison: the
/// choreographed saga has nothing equivalent to this. If Payments.Service
/// never replies, an order there just stays "Created" forever with no
/// automatic detection - here, the orchestrator itself owns noticing and
/// acting on that.
///
/// Milestone 36: gated on LeaderElectionService.IsLeader so only one
/// orders-worker replica actively sweeps at a time - every replica still
/// runs this loop, but non-leaders no-op each tick rather than each
/// redundantly scanning the same rows.
/// </summary>
public sealed class SagaTimeoutSweeper(
    IOptions<SagaOrchestrationOptions> options,
    SagaOrchestrationStore store,
    LeaderElectionService leaderElection,
    ILogger<SagaTimeoutSweeper> logger) : BackgroundService
{
    private const int SweepBatchSize = 100;
    private readonly SagaOrchestrationOptions _options = options.Value;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var timeout = TimeSpan.FromSeconds(_options.TimeoutSeconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(_options.SweepIntervalMilliseconds, stoppingToken);

            if (!leaderElection.IsLeader)
            {
                continue;
            }

            var timedOut = await store.ClaimTimedOutAsync(timeout, DateTimeOffset.UtcNow, SweepBatchSize, stoppingToken);
            foreach (var (orderId, saga) in timedOut)
            {
                SagaOrchestratorLog.SagaTimedOut(logger, orderId, _options.TimeoutSeconds, saga.CorrelationId);
            }
        }
    }
}
