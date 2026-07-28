namespace Payments.Service.Domain;

public sealed class Payment
{
    private Payment()
    {
    }

    public Guid Id { get; private set; }

    public Guid OrderId { get; private set; }

    public decimal Amount { get; private set; }

    public string Currency { get; private set; } = string.Empty;

    public bool Approved { get; private set; }

    public DateTimeOffset DecidedAt { get; private set; }

    public string CorrelationId { get; private set; } = string.Empty;

    public static Payment Decide(
        Guid orderId,
        decimal amount,
        string currency,
        bool approved,
        DateTimeOffset decidedAt,
        string correlationId)
    {
        return new Payment
        {
            Id = Guid.NewGuid(),
            OrderId = orderId,
            Amount = amount,
            Currency = currency,
            Approved = approved,
            DecidedAt = decidedAt,
            CorrelationId = correlationId
        };
    }
}
