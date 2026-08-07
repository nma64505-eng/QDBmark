# Directory Structure

This repository keeps runtime code, frontend assets, deployment files, and benchmark tools separated so it is clear where a change belongs.

## Top Level

```text
app.py
backend/
frontend/
tools/
docs/
Dockerfile
docker-compose.yml
requirements.txt
start-db-benchmark.sh
```

- `app.py`: small WSGI entrypoint used by the container runtime.
- `backend/`: all Python application and benchmark orchestration code.
- `frontend/`: HTML templates, CSS, images, and report template assets.
- `tools/`: bundled benchmark engines and offline tool caches used by the Docker build.
- `docs/`: project documentation for maintainers.
- `Dockerfile`: canonical image build entrypoint.
- `docker-compose.yml`: development and deployment compose file.
- `requirements.txt`: Python dependency list.
- `start-db-benchmark.sh`: direct `docker run` helper kept for compatibility.

## Backend

```text
backend/db_benchmark/
├── app.py
├── benchmark.py
├── configs.py
├── prosql.py
├── report.py
└── databases/
```

- `app.py`: HTTP APIs, task creation, task state, settings, and report download endpoints.
- `benchmark.py`: shared execution helpers, connectivity tests, result parsing, cleanup, and report data assembly.
- `configs.py`: converts UI/API payloads into normalized benchmark configuration objects.
- `prosql.py`: Prometheus queries and runtime metric sampling.
- `report.py`: DOCX/PDF report generation.
- `databases/`: per-database runner modules. New database or middleware support should start here.

## Frontend

```text
frontend/
├── static/
├── templates/
└── report_template.docx
```

- `static/`: CSS, SVG, PNG, and other static assets.
- `templates/`: Flask-rendered pages.
- `report_template.docx`: report template asset.

## Tools

```text
tools/
├── benchmarksql-gaussdb/
├── benchmarksql-kingbase/
├── benchmarksql-oceanbase-src/
├── build-scripts/
├── hammerdb-oracle-sqlserver-cache/
├── instantclient-oracle-cache/
├── rabbitmq-perf-test/
├── swingbench-oracle-cache/
└── sysbench-mysql-postgresql-vastbase/
```

- `benchmarksql-*`: BenchmarkSQL variants and database-specific adapters.
- `build-scripts/`: scripts used during image build or vendor patching.
- `hammerdb-oracle-sqlserver-cache/`: HammerDB offline archives.
- `instantclient-oracle-cache/`: Oracle Instant Client offline archive.
- `benchmarksql-dm-cache/`: optional local-only DM BenchmarkSQL archive cache.
- `rabbitmq-perf-test/`: RabbitMQ performance test jar.
- `swingbench-oracle-cache/`: Swingbench offline archive.
- `sysbench-mysql-postgresql-vastbase/`: sysbench notes and vendored runtime layout.

The extracted `tools/benchmarksql-dm/` runtime directory is intentionally ignored because it includes generated runtime files and certificates. Public source does not commit the DM vendor archive. If DM BenchmarkSQL support is required during local image builds, place the archive at `tools/benchmarksql-dm-cache/benchmarksql-dm-x86.tar`.

## Ignored Runtime Content

These paths should not be committed:

- `data/`
- `.codex-backup/`
- `frontend-backups/`
- `image/`
- generated package archives
- generated BenchmarkSQL result directories
- `__pycache__/` and Python bytecode
- private keys, certificates, and local credentials
