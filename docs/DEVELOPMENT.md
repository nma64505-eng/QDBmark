# Development

## Build and Run

```bash
docker compose build
docker compose up -d
```

The service listens on port `12365` by default.

```bash
curl http://127.0.0.1:12365/health
```

## Python Syntax Check

```bash
python3 -m compileall -q backend app.py
```

## Adding a Database Runner

1. Add the runner module under `backend/db_benchmark/databases/<engine>/`.
2. Register dispatch logic in `backend/db_benchmark/databases/dispatcher.py`.
3. Add request/config fields in `backend/db_benchmark/configs.py`.
4. Add execution, parsing, and cleanup logic in `backend/db_benchmark/benchmark.py` only when it is shared or legacy-path compatible.
5. Add frontend fields and labels under `frontend/templates/` and `frontend/static/`.
6. Add report output mappings in `backend/db_benchmark/report.py` when needed.

## Git Hygiene

Before pushing:

```bash
git status --short
find . -type f -size +95M -not -path './.git/*' -print
rg --pcre2 'BEGIN (RSA|OPENSSH|DSA|EC) PRIVATE KEY|ghp_[A-Za-z0-9_]+|github_pat_' .
```

Runtime data, reports, backups, generated archives, and credentials must stay out of Git.
