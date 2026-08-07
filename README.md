# QDBmark

QDBmark is a database and middleware benchmark platform. It provides a web UI for creating benchmark tasks, running connectivity checks, collecting Prometheus/container metrics, and exporting performance reports.

## Supported Targets

- Relational databases: MySQL, TiDB, SQL Server, Oracle, PostgreSQL, GaussDB, OpenGauss, DM, OceanBase, KingBase, Vastbase
- NoSQL and search: MongoDB, Redis, Elasticsearch, ClickHouse
- Messaging: Kafka, RocketMQ, RabbitMQ
- Storage/file benchmark: FIO

## Repository Layout

```text
.
├── app.py                         # WSGI entrypoint
├── backend/                       # Python backend and benchmark runners
│   └── db_benchmark/
│       ├── app.py                 # Flask routes and task orchestration APIs
│       ├── benchmark.py           # Shared benchmark execution logic
│       ├── configs.py             # Request/config normalization
│       ├── prosql.py              # Prometheus and runtime metric helpers
│       ├── report.py              # Report generation
│       └── databases/             # Per-engine runner modules
├── frontend/                      # Templates and static UI assets
│   ├── static/
│   └── templates/
├── tools/                         # Benchmark engines, tool source, and offline caches
├── docs/                          # Architecture and maintenance documentation
├── Dockerfile                     # Container image build
├── docker-compose.yml             # Local/container deployment
├── requirements.txt               # Python dependencies
└── start-db-benchmark.sh          # Direct docker-run helper
```

More details are in [docs/DIRECTORY_STRUCTURE.md](docs/DIRECTORY_STRUCTURE.md).

## Local Build

```bash
docker compose build
docker compose up -d
```

Default URL:

```text
http://127.0.0.1:12365
```

Health check:

```bash
curl http://127.0.0.1:12365/health
```

## Release Package

End users should prefer the packaged release from GitHub Releases. The release archive includes:

- `start.sh`
- `uninstall.sh`
- `docker-compose.yml`
- packaged `docker-compose`
- Docker image archive
- usage guide

For hosts whose Docker bridge subnet conflicts with the host network, set `QDBMARK_DOCKER_SUBNET` and `QDBMARK_DOCKER_GATEWAY` before running `start.sh`.

## Docker Network

QDBmark uses a custom Docker bridge network by default to avoid conflicts with common host-side `172.x` networks:

```text
network: qdbmark_net
subnet: 10.245.0.0/24
gateway: 10.245.0.1
```

Override it when starting the package:

```bash
QDBMARK_DOCKER_NETWORK=qdbmark_net_2 \
QDBMARK_DOCKER_SUBNET=10.246.0.0/24 \
QDBMARK_DOCKER_GATEWAY=10.246.0.1 \
./start-db-benchmark.sh
```

The same variables are supported by `docker compose`:

```bash
QDBMARK_DOCKER_SUBNET=10.246.0.0/24 \
QDBMARK_DOCKER_GATEWAY=10.246.0.1 \
docker compose up -d --build
```

## Runtime Data

The application writes reports, uploaded files, task artifacts, and settings under `data/`. This directory is intentionally ignored by Git:

```text
data/reports
data/uploads
data/artifacts
data/settings
```

## Tooling Notes

Some benchmark tools are included as source directories or offline archives so the Docker build can run in restricted environments. Generated output directories, unpacked runtime directories containing generated certificates, historical result folders, and release image packages are excluded from the repository.

The DM BenchmarkSQL runtime archive is not committed to the public repository because the vendor archive may contain generated SSL key/certificate material. To build an image with DM BenchmarkSQL support from source, place your legally obtained archive at:

```text
tools/benchmarksql-dm-cache/benchmarksql-dm-x86.tar
```

Then run `docker compose build`.

## Security Notes

Do not commit customer data, generated reports, Prometheus settings, database passwords, SSH credentials, private keys, or package image exports. Example BenchmarkSQL property files use `password=CHANGE_ME`.

## License

QDBmark source code is released under the Apache License 2.0. Third-party benchmark engines, database clients, and tools keep their own upstream licenses.
