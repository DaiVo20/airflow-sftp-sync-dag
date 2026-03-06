from __future__ import annotations

import posixpath
import stat as statmod
from datetime import timedelta

import pendulum
from airflow.models import Variable
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.sdk import dag, task
from airflow.task.trigger_rule import TriggerRule

from target_storage.storage_targets import TargetKind, TargetConfig, build_target

# =========================
# Config
# =========================
DAG_ID = "sftp_sync_files_dag"

DEFAULT_ARGS = {
    "owner": "Dai Vo",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

SOURCE_CONN_ID = "sftp-source"
# Root sync directory on each SFTP server
SOURCE_SFTP_ROOT = "/upload"

TARGET_SFTP_ROOT = "/upload"
TARGET_SFTP_CONN_ID = "sftp-target"  # used when target_kind = "sftp"

AWS_CONN_ID_DEFAULT = "aws_default"  # used when target_kind = "s3"
SYNC_S3_BUCKET = "sftp-sync"  # used when target_kind = "s3"
SYNC_S3_PREFIX = ""  # used when target_kind = "s3"

STATE_VAR_KEY = "sftp_sync_state__source_to_target"

POKE_INTERVAL = 30
SENSOR_TIMEOUT = 60 * 60

# Split threshold
LARGE_FILE_THRESHOLD_BYTES = 100 * 1024 * 1024  # 100 MB


# =========================
# Helpers
# =========================
def _norm(path: str) -> str:
    return posixpath.normpath(path)


def _is_under(root: str, path: str) -> bool:
    root_n = _norm(root).rstrip("/")
    path_n = _norm(path)
    return path_n == root_n or path_n.startswith(root_n + "/")


def _walk_sftp_files_recursive(sftp_client, root: str) -> dict[str, dict[str, int]]:
    """
    Recursively scan files under root and return snapshot:
    {
      "/upload/a/b/c/file.txt": {"size": 123, "mtime": 1700000000}
    }
    """
    root = _norm(root)
    snapshot: dict[str, dict[str, int]] = {}

    def _walk(dir_path: str) -> None:
        for attr in sftp_client.listdir_attr(dir_path):
            child_path = _norm(posixpath.join(dir_path, attr.filename))
            if statmod.S_ISDIR(attr.st_mode):
                _walk(child_path)
            elif statmod.S_ISREG(attr.st_mode) and _is_under(root, child_path):
                snapshot[child_path] = {
                    "size": int(attr.st_size),
                    "mtime": int(attr.st_mtime),
                }

    _walk(root)
    return snapshot


def _build_target_from_variables(target_kind: TargetKind):
    """
    Creates and returns a target storage backend (SFTP or S3) based on the specified
    target_kind parameter. Configuration values are retrieved from Airflow variables
    and constants defined at the module level.

    Supports two target types:
    - SFTP: Syncs files to a remote SFTP server using SSH connection credentials
    - S3: Syncs files to an AWS S3 bucket using AWS connection credentials

    Args:
        target_kind (TargetKind): Enum value specifying the target type (SFTP or S3)

    Returns:
        StorageTarget: An initialized target instance (SFTPTarget or S3Target) ready
                       to accept file uploads via the put_stream() method

    Raises:
        ValueError: If target_kind is not a valid TargetKind enum value
    """

    cfg = TargetConfig(
        kind=target_kind,
        # SFTP target
        sftp_conn_id=TARGET_SFTP_CONN_ID,
        sftp_root=TARGET_SFTP_ROOT,
        # S3 target
        aws_conn_id=AWS_CONN_ID_DEFAULT,
        s3_bucket=SYNC_S3_BUCKET,
        s3_prefix=SYNC_S3_PREFIX,
    )
    return build_target(cfg)


# =========================
# DAG
# =========================
@dag(
    dag_id=DAG_ID,
    description="One-way sync from SFTP source to pluggable target (SFTP or S3), with small/large lanes",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 3, 5, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["sftp", "sync"],
)
def sftp_sync_dag():
    # Sensor = polling gate only.
    # Recursive diff and actual sync are done in the Python task below.
    wait_for_new_or_updated = SFTPSensor(
        task_id="wait_for_new_or_updated",
        sftp_conn_id=SOURCE_CONN_ID,
        path=SOURCE_SFTP_ROOT,
        file_pattern="*",
        poke_interval=POKE_INTERVAL,
        timeout=SENSOR_TIMEOUT,
        deferrable=True,
    )

    @task(task_id="discover_changes")
    def discover_changes() -> dict:
        """
        Scan source recursively, compute changed files (size+mtime),
        then split into small/large lanes. No delete propagation by design.
        """
        previous_state = Variable.get(STATE_VAR_KEY, default_var={}, deserialize_json=True)

        src = SFTPHook(ssh_conn_id=SOURCE_CONN_ID).get_conn()
        current_state = _walk_sftp_files_recursive(src, SOURCE_SFTP_ROOT)

        small_files: list[dict] = []
        large_files: list[dict] = []

        for source_path in sorted(current_state.keys()):
            cur_fp = current_state[source_path]
            prev_fp = previous_state.get(source_path)

            if prev_fp == cur_fp:
                continue  # unchanged

            rel_path = posixpath.relpath(_norm(source_path), _norm(SOURCE_SFTP_ROOT))

            item = {
                "source_path": source_path,
                "rel_path": rel_path,
                "size": cur_fp["size"],
                "mtime": cur_fp["mtime"],
            }

            if cur_fp["size"] >= LARGE_FILE_THRESHOLD_BYTES:
                large_files.append(item)
            else:
                small_files.append(item)

        summary = {
            "scanned_files": len(current_state),
            "changed_files": len(small_files) + len(large_files),
            "small_files": len(small_files),
            "large_files": len(large_files),
            "threshold_bytes": LARGE_FILE_THRESHOLD_BYTES,
            "note": "No delete propagation from source to target.",
        }
        print({"discover_summary": summary})

        return {
            "current_state": current_state,
            "small_files": small_files,
            "large_files": large_files,
            "summary": summary,
        }

    @task(task_id="get_small_files")
    def get_small_files(discovery: dict) -> list[dict]:
        return discovery["small_files"]

    @task(task_id="get_large_files")
    def get_large_files(discovery: dict) -> list[dict]:
        return discovery["large_files"]

    @task(
        task_id="copy_small_file",
        pool="sftp_small_pool",
        execution_timeout=timedelta(minutes=10),
        retries=2,
        retry_delay=timedelta(minutes=1),
    )
    def copy_small_file(item: dict) -> dict:
        src = SFTPHook(ssh_conn_id=SOURCE_CONN_ID).get_conn()
        target = _build_target_from_variables(TargetKind.SFTP)

        with src.open(item["source_path"], "rb") as stream:
            target.put_stream(item["rel_path"], stream, size=int(item["size"]))

        dest = target.dest_ref(item["rel_path"]).identifier
        return {"status": "copied", "dest": dest, **item}

    @task(
        task_id="copy_large_file",
        pool="sftp_large_pool",
        execution_timeout=timedelta(hours=2),
        retries=3,
        retry_delay=timedelta(minutes=5),
    )
    def copy_large_file(item: dict) -> dict:
        src = SFTPHook(ssh_conn_id=SOURCE_CONN_ID).get_conn()
        target = _build_target_from_variables(TargetKind.SFTP)

        with src.open(item["source_path"], "rb") as stream:
            target.put_stream(item["rel_path"], stream, size=int(item["size"]))

        dest = target.dest_ref(item["rel_path"]).identifier
        return {"status": "copied", "dest": dest, **item}

    @task(task_id="finalize_state", trigger_rule=TriggerRule.NONE_FAILED)
    def finalize_state(discovery: dict) -> dict:
        """
        Update state only if no upstream failures.
        Works even if mapped lanes are empty (skipped).
        """
        Variable.set(STATE_VAR_KEY, discovery["current_state"], serialize_json=True)
        out = {**discovery["summary"], "state_updated": True}
        print({"finalize_summary": out})
        return out

    discovery = discover_changes()

    small_list = get_small_files(discovery)
    large_list = get_large_files(discovery)

    copied_small = copy_small_file.expand(item=small_list)
    copied_large = copy_large_file.expand(item=large_list)

    final = finalize_state(discovery)

    wait_for_new_or_updated >> discovery
    copied_small >> final
    copied_large >> final

dag = sftp_sync_dag()
