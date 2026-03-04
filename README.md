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

### Stop services (optional)

```bash
docker compose -p local-sync down
docker compose -p local-sync -f docker-compose-sftp.yaml down
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

