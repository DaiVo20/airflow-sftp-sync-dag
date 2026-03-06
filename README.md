# airflow-sftp-sync-dag
Apache Airflow DAG for one-way SFTP-to-SFTP file synchronization with preserved directory structure.

## Overview

This project implements a **unidirectional file synchronization workflow** between two SFTP servers:

- **Source**: `<source>`
- **Target**: `<target>`

The DAG copies files from **source** to **target** while preserving the original directory structure.

### Required behaviors

- Copy files from source to target
- Preserve directory structure (e.g. `/a/b/c/file.txt` -> `/a/b/c/file.txt`)
- One-way sync only (**changes on target must not affect source**)
- Files deleted from source must remain on target (**no delete propagation**)

---


## Setup Infrastructure (Airflow + 2 SFTP Servers)
### Airflow Runtime
- Airflow image: `apache/airflow:3.1.7` (pin for reproducibility)
- Executor: CeleryExecutor
- Metadata DB: PostgreSQL (SQLite is permissible, but Postgres is used for better concurrency support)
- Broker: Redis (for Celery task queue) but can be swapped with RabbitMQ for heavier workloads

### DAG Configuration
- DAG ID: `sftp_sync_dag`
- Airflow Connections:
  - `sftp_source` (Host: `sftp-source`, Port: 22)
  - `sftp_target` (Host: `sftp-target`, Port: 22)
- Sync roots:
  - Source root: `/upload`
  - Target root: `/upload`
- Sync state: stored in Airflow Variables (see `sftp_sync_state__source_to_target` in DAG) to track last known file states for idempotency and change detection. In production, a more robust external state store (e.g. Redis, Postgres) may be preferable.

### Local SFTP Notes
For local development, you may disable host key checking via Connection Extra:
`{"no_host_key_check": true}`

### Docker Compose Setup
This project uses two Docker Compose files:

- `docker-compose-sftp.yaml` for local SFTP source/target servers
- `docker-compose.yml` for the Airflow cluster

### 1) Start SFTP servers (source + target)

```bash
docker compose -p local-sync -f docker-compose-sftp.yaml up -d --build
```

### 2) Start Airflow cluster

```bash
# set AIRFLOW_UID to current user id for proper file permissions
echo "AIRFLOW_UID=$(id -u)" > .env

docker compose -p local-sync up -d
```

### 3) Import Airflow connections
```bash
docker compose -p local-sync run --rm airflow-cli airflow connections import --overwrite /opt/airflow/plugins/connections.json
```

### 4) Create pool for small and large files transfers
```bash
docker compose -p local-sync run --rm airflow-cli airflow pools set sftp_small_pool 32 "High concurrency for small files" 
docker compose -p local-sync run --rm airflow-cli airflow pools set sftp_large_pool 8 "High concurrency for large files" 
```

### Stop services (optional)

```bash
docker compose -p local-sync -f docker-compose-sftp.yaml down
docker compose -p local-sync down
```
---

## How It Works (High Level)

The DAG follows this general flow:

1. Recursively scan files from the source SFTP root
2. Build relative paths from source root
3. Create missing directories on target
4. Transfer files from source to target while preserving the original path
5. Skip deletion logic (target retains files even if removed from source)


---


## Local Utility Scripts (uv)

This repository includes local utility scripts (e.g. test file generation) that can be run with [`uv`](https://docs.astral.sh/uv/).

### Setup `uv` (with standalone installers)
```bash
# On macOS and Linux.
curl -LsSf https://astral.sh/uv/install.sh | sh
```

```bash
# On Windows.
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### Create a local Python environment

```bash
uv venv
```

Activate it (optional):

```bash
source .venv/bin/activate
```

> The current utility script (`scripts/generate_test_files.py`) uses only Python standard library, so no additional dependencies are required.

### Run the test file generator with `uv`

Generate sample files in `data/source/a/b/c` (default behavior):

```bash
uv run scripts/generate_test_files.py
```

### Example: customize generated files

Generate more small files and multiple large files (~100MB each):

```bash
uv run scripts/generate_test_files.py --small-count 10 --large-count 2 --large-base-mb 100 --large-jitter-mb 5
```

Generate files in a custom subdirectory:

```bash
uv run scripts/generate_test_files.py --root data/source --subdir incoming/2026/03/05
```

### Notes

- These scripts are intended for **local testing/debugging** only.
- They are kept outside the Airflow DAG to keep the DAG focused on the SFTP sync workflow.

---

## Assumptions

- Source and target SFTP credentials are provided through **Airflow Connections**
- The synchronization is **unidirectional only** (`source -> target`)
- The target acts as a retained copy destination (deletions on source are ignored)
- The DAG is expected to be re-runnable (idempotent behavior is preferred)
- Network/SFTP transient failures are handled with Airflow retries

---

## Design Decisions & Trade-offs

### 1) One-way synchronization
This directly matches the problem statement and avoids unnecessary bidirectional conflict handling.

### 2) Preserve directory structure
Using relative path mapping keeps the behavior deterministic and easy to reason about.

### 3) Extensibility-oriented design
The implementation should separate concerns (listing, transfer, path mapping, state handling) so future changes (e.g. SFTP -> Object Storage) are easier to support.

### 4) Scalability considerations
For larger files (KB -> GB growth), streaming/chunked transfer is preferable to loading entire files into memory.

