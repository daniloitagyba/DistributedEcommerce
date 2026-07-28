namespace Orders.Application.Exceptions;

/// <summary>
/// Raised by a Ports implementation when the underlying infrastructure (PostgreSQL,
/// Redis, Kafka, ...) is unavailable, translating a technology-specific fault into a
/// signal the Api layer can map to a 503 without knowing which technology failed.
/// </summary>
public sealed class InfrastructureUnavailableException(string message, Exception innerException)
    : Exception(message, innerException);
