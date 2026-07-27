# Repository Guidance

All source code, builds, containers, Kubernetes commands, logs, and tests run on the Ubuntu server in `/srv/local-distributed-lab`.

## Engineering standards

- Write code, identifiers, comments, documentation, and configuration names in English.
- Keep nullable reference types enabled and treat warnings as errors.
- Use asynchronous APIs for I/O and propagate cancellation tokens.
- Use dependency injection and structured `ILogger` logging.
- Never log credentials or commit `compose/.env`.
- Keep PostgreSQL, Kafka, and administrative interfaces off public interfaces.
- Add focused tests for important behavior and use Testcontainers for external dependencies where practical.
- Preserve graceful shutdown and meaningful liveness/readiness checks.

## Operational safety

Do not use `sudo`, delete files or volumes, prune Docker, delete Kubernetes resources, alter SSH/firewall/systemd/host settings, or commit/push without explicit approval.

Run Compose commands from `compose/` so Docker Compose loads the untracked `.env` file automatically.
