# Contributing

Thanks for improving QDBmark.

## Development

1. Create a branch from `main`.
2. Keep changes scoped to one feature or fix.
3. Run syntax checks and targeted tests before opening a pull request.
4. Do not commit generated reports, runtime data, customer files, private keys, credentials, or exported Docker images.

## Project Layout

- `backend/db_benchmark/`: backend APIs, task orchestration, report generation, and per-engine runners.
- `frontend/`: templates, styles, and static UI assets.
- `tools/`: benchmark engines, source trees, and allowed offline dependency caches.
- `docs/`: architecture and maintenance notes.

## Third-Party Tools

Third-party benchmark tools and database clients keep their own licenses. Only add redistributable artifacts to this repository. If a tool archive contains generated keys, certificates, credentials, or non-redistributable content, document where to place it locally instead of committing it.
