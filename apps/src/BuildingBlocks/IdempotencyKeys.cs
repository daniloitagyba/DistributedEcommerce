namespace BuildingBlocks;

public static class IdempotencyKeys
{
    public static string Key(string idempotencyKey) => $"orders:idempotency:{idempotencyKey}";

    public static string LockKey(string idempotencyKey) => $"orders:idempotency-lock:{idempotencyKey}";
}
