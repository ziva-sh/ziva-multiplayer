# Security Policy

## Reporting a vulnerability

Please email **security@ziva.sh** with a description of the issue and steps to
reproduce. We aim to acknowledge within 2 business days.

Do **not** file public GitHub issues for security problems.

## Scope

In scope:

- The relay Worker and `RoomDO` in this repo.
- The JWT verification path and any code that touches the wire protocol.

Out of scope:

- Issues that require a compromised `MULTIPLAYER_JWT_SECRET` (the secret is
  trusted; protecting it is the issuer's responsibility).
- Denial-of-service from a single authenticated client beyond the documented
  per-connection rate limits.
- Vulnerabilities in third-party services (Cloudflare, GitHub, etc.) — report
  those upstream.

## Disclosure

We coordinate disclosure once a fix is shipped. Credit is offered unless the
reporter requests otherwise.
