# Contributing

Thanks for your interest in improving the Ziva multiplayer relay.

## Ground rules

- Discuss non-trivial changes in an issue first.
- Keep PRs focused. One concern per PR.
- Tests are required for behaviour changes. CI runs `bun test` against the
  real Workers runtime via `@cloudflare/vitest-pool-workers`.
- Don't add silent fallback paths. Fail loudly with actionable errors.

## Workflow

1. Fork the repo, branch from `main`.
2. `cd worker && bun install`.
3. Make your change. Add or update tests in `worker/test/`.
4. `bun test` — must pass.
5. Open a PR. CI runs `ci.yml` on every push.

## Wire protocol changes

The wire protocol is versioned via the `?v=<major>` query parameter. Breaking
changes require a new major version and updates to [`PROTOCOL.md`](./PROTOCOL.md).

## Releases

Releases are tagged on `main`. The `deploy-prod.yml` workflow runs on tag push
and gates on a manual approval (`environment: production`) before invoking
`wrangler deploy`.
