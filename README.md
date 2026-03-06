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
- DAG ID: `sftp_sync_files_dag`
- Airflow Connections:
  - `sftp-source` (Host: `sftp-source`, Port: 22)
  - `sftp-target` (Host: `sftp-target`, Port: 22)
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

### 1) One-way synchronization (source ➜ target only)
**Decision:** The DAG is intentionally **unidirectional**. Changes made on the target do not affect the source, and deletions on the source are not propagated to the target.  
**Why:** This matches the assignment requirements and simplifies conflict handling (no bidirectional reconciliation).  
**Trade-off:** Target can diverge from source if manually modified; this is acceptable by design.

### 2) Preserve directory structure using relative path mapping
**Decision:** The DAG computes a `rel_path` from `SOURCE_ROOT` and writes to the target using the same relative path.  
**Why:** This guarantees deterministic placement and makes verification simple (path is the contract).  
**Trade-off:** Any change in source root definition changes the mapping; therefore `SOURCE_ROOT` is treated as a stable configuration boundary.

### 3) Incremental sync using metadata snapshot (size + mtime)
**Decision:** The workflow tracks a snapshot of files on the source (path ➜ `{size, mtime}`) and only transfers files that are new or have changed. Snapshot state is stored in **Airflow Variables**.  
**Why:** This keeps the solution lightweight and avoids hashing every file on every run, while still enabling incremental behavior.  
**Trade-off:** Metadata comparison is not as strict as checksums (rare edge cases exist where content changes without metadata differences). For stricter integrity, checksum verification can be added for selected files (e.g., large files only).

### 4) Sensor as a gate; authoritative discovery in a Python task
**Decision:** `SFTPSensor` is used as a **polling gate**, while the recursive scan and diffing are done in a dedicated discovery task.  
**Why:** Sensors are good for scheduling/polling, but the required behavior (recursive traversal + incremental diff + lane split) is better controlled explicitly in code.  
**Trade-off:** This introduces an extra scan step in the DAG, but it keeps behavior consistent and debuggable.

### 5) Split processing into small-file and large-file lanes (resource-aware orchestration)
**Decision:** Changed files are partitioned into **small** and **large** lanes based on a size threshold. Each lane uses different pools/timeouts/retries.  
**Why:** This is a scalable strategy when file sizes grow from KB to GB:
- small files can run with higher concurrency
- large files should be throttled to avoid saturating bandwidth/disk and to reduce blast radius on failures  
**Trade-off:** More orchestration complexity (two mapped tasks + pools), but improved operational safety and clearer scaling knobs.

### 6) Disk-spooled transfers to avoid memory pressure
**Decision:** File transfers are performed using a **stream ➜ temporary local file ➜ upload** approach.  
**Why:** This prevents loading entire files into memory, which is critical when files become large.  
**Trade-off:** Requires temporary disk space on workers; for very large files, disk sizing and cleanup become operational considerations.

### 7) State finalization only after successful transfers
**Decision:** The state snapshot is updated only after all file transfer tasks complete successfully.  
**Why:** Prevents marking files as “synced” when a transfer fails midway; supports safe retries by reprocessing unresolved items in the next run.  
**Trade-off:** A single failing large file can delay state advancement for that run, which is preferable to silently skipping data.

### 8) Extensibility via Target Storage abstraction
**Decision:** Target write operations are abstracted behind a `TargetStorage` interface with an enum-like selector (`TargetKind`).  
**Why:** This makes the solution adaptable if the target changes from **SFTP ➜ Object Storage (S3/MinIO)** without rewriting the DAG’s orchestration logic.  
**Trade-off:** Additional code structure (adapters/factory), but significantly improved adaptability and testability.

### 9) Transformation-friendly architecture
**Decision:** The pipeline is structured as discover ➜ plan ➜ transfer ➜ finalize.  
**Why:** This enables inserting optional transformations between “read” and “write” (validation, rename, compression, encryption, etc.) without changing the discovery/state logic.  
**Trade-off:** Slightly more boilerplate than a single monolithic task, but improves maintainability.

### 10) Handling anomalies and scale changes (KB ➜ GB)
**Decision:** The design favors throttling and robustness over raw throughput:
- dedicated pools for large files
- longer timeouts and retries for large transfers
- disk-spooled approach to avoid memory spikes  
**Trade-off:** Lower peak throughput for very large files, but predictable performance and fewer operational surprises.

## Future Improvements (Large File Scale: KB ➜ GB)

### 1) Multipart upload tuning for S3 / MinIO
For large files, S3-compatible backends typically use multipart upload. The following tuning knobs can improve throughput and reliability depending on network and worker resources:

- **`multipart_threshold`**: size threshold to start multipart uploads (e.g., 16–64 MB).
- **`multipart_chunksize`**: part size (e.g., 8–64 MB). Larger parts reduce request overhead; smaller parts improve retry granularity.
- **`max_concurrency`**: number of parallel part uploads. This should be balanced with worker bandwidth and the large-file pool size.
- **Retry strategy**: multipart enables retrying failed parts instead of re-uploading the entire object.

In Airflow, these settings can be applied via boto3 transfer configuration (e.g., `TransferConfig`) or hook/operator parameters when available, so behavior can be tuned without changing DAG orchestration.

### 2) File stability check (avoid copying files still being written)
When files are large, there is a higher chance the file is still being uploaded to the source SFTP while the DAG scans it. A stability check reduces partial transfers:

- **Two-pass metadata check**: record `(size, mtime)` for candidates, sleep for a short window (e.g., 30–60s), re-check. Only transfer if unchanged.
- **Minimum age threshold**: only transfer files with `mtime <= now - N minutes`.
- **Optional marker file convention**: if upstream can create `.done` markers, only transfer files with corresponding completion markers.

This prevents copying incomplete files and reduces wasted network/compute.

### 3) Resume support for interrupted large transfers
For GB-scale files, retries may be expensive if re-transfer always starts from byte 0. Resume support improves robustness:

- **S3/MinIO target**: multipart upload already supports part-level retries; the pipeline can persist upload state (upload ID / completed parts) for recovery.
- **SFTP target**: write to a temporary file (e.g., `file.tmp`) and resume from the last byte offset if the server supports appending, then atomically rename to the final name.
- **State tracking**: persist transfer progress metadata (e.g., bytes completed, last successful part) in an external store (DB/table/Redis) to allow safe continuation across task retries or reruns.

Resume support would be applied only to the large-file lane to keep small-file handling simple and fast.