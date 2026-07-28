namespace Payments.Service;

public sealed class PaymentDecisionOptions
{
    public const string SectionName = "PaymentDecision";

    /// <summary>
    /// Orders with an amount strictly greater than this threshold are declined.
    /// A deterministic rule keeps the saga experiment reproducible.
    /// </summary>
    public decimal DeclineAmountThreshold { get; init; } = 1_000m;
}
