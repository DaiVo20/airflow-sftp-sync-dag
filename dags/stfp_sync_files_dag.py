from __future__ import annotations

import os
import posixpath
import stat as statmod
import tempfile
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.sdk import dag, task

# =========================
# Config
# =========================
DAG_ID = "sftp_sync_files_dag"

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

# Root sync directory on each SFTP server
SOURCE_ROOT = "/upload"
TARGET_ROOT = "/upload"

SOURCE_CONN_ID = "sftp-source"
TARGET_CONN_ID = "sftp-target"

# Persist source snapshot (for incremental detection)
STATE_VAR_KEY = "sftp_sync_state__source_to_target"

POKE_INTERVAL = 30
SENSOR_TIMEOUT = 60 * 60


# =========================
# Helpers
# =========================
def _norm(path: str) -> str:
    return posixpath.normpath(path)


def _is_under(root: str, path: str) -> bool:
    root_n = _norm(root).rstrip("/")
    path_n = _norm(path)
    return path_n == root_n or path_n.startswith(root_n + "/")


def _target_from_source(source_path: str) -> str:
    rel_path = posixpath.relpath(_norm(source_path), _norm(SOURCE_ROOT))
    return _norm(posixpath.join(TARGET_ROOT, rel_path))


def _mkdir_p_sftp(sftp_client, remote_dir: str) -> None:
    """Recursively create directory on target SFTP."""
    remote_dir = _norm(remote_dir)
    if remote_dir in ("", "/"):
        return

    parts = remote_dir.strip("/").split("/")
    cur = "/"
    for part in parts:
        cur = posixpath.join(cur, part)
        try:
            st = sftp_client.stat(cur)
            if not statmod.S_ISDIR(st.st_mode):
                raise RuntimeError(f"Target path exists but is not a directory: {cur}")
        except FileNotFoundError:
            sftp_client.mkdir(cur)


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
            elif statmod.S_ISREG(attr.st_mode):
                if _is_under(root, child_path):
                    snapshot[child_path] = {
                        "size": int(attr.st_size),
                        "mtime": int(attr.st_mtime),
                    }

    _walk(root)
    return snapshot


def _copy_one_file_via_temp(src_sftp_client, tgt_sftp_client, source_path: str, target_path: str) -> None:
    """
    Copy source SFTP file -> target SFTP via local temp file.
    Disk-spooled approach avoids loading entire file into memory.
    """
    _mkdir_p_sftp(tgt_sftp_client, posixpath.dirname(target_path))

    fd, tmp_path = tempfile.mkstemp(prefix="sftp_sync_", suffix=".tmp")
    os.close(fd)

    try:
        src_sftp_client.get(source_path, tmp_path)
        tgt_sftp_client.put(tmp_path, target_path, confirm=True)
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


# =========================
# DAG
# =========================
@dag(
    dag_id=DAG_ID,
    description="One-way SFTP-to-SFTP sync (single flow) with preserved directory structure",
    schedule=None,
    start_date=pendulum.datetime(2026, 3, 5, tz="Asia/Ho_Chi_Minh"),
    catchup=True,
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
        path=SOURCE_ROOT,
        file_pattern="*",
        poke_interval=POKE_INTERVAL,
        timeout=SENSOR_TIMEOUT,
        deferrable=True,
    )

    @task(task_id="sync_changed_files")
    def sync_changed_files() -> dict:
        """
        One-way incremental sync:
        - source -> target
        - preserve directory structure
        - no delete propagation
        - copy new/updated files based on (size, mtime)
        """
        previous_state = Variable.get(
            STATE_VAR_KEY,
            default_var={},
            deserialize_json=True,
        )

        src_hook = SFTPHook(ssh_conn_id=SOURCE_CONN_ID)
        tgt_hook = SFTPHook(ssh_conn_id=TARGET_CONN_ID)

        src = src_hook.get_conn()
        tgt = tgt_hook.get_conn()

        # Current source snapshot (recursive)
        current_state = _walk_sftp_files_recursive(src, SOURCE_ROOT)

        copied = []
        skipped = []
        failed = []

        for source_path in sorted(current_state.keys()):
            current_fp = current_state[source_path]
            previous_fp = previous_state.get(source_path)

            # unchanged -> skip
            if previous_fp == current_fp:
                skipped.append(source_path)
                continue

            target_path = _target_from_source(source_path)

            try:
                _copy_one_file_via_temp(src, tgt, source_path, target_path)
                copied.append({
                    "source": source_path,
                    "target": target_path,
                    "size": current_fp["size"],
                    "mtime": current_fp["mtime"],
                })
            except Exception as e:
                failed.append({
                    "source": source_path,
                    "target": target_path,
                    "error": str(e),
                })

        # IMPORTANT:
        # - Do NOT delete on target when source file is deleted
        # - Update state only if all changed files copied successfully
        if failed:
            raise RuntimeError(
                f"SFTP sync failed for {len(failed)} file(s). "
                f"copied={len(copied)}, skipped={len(skipped)}, failed={len(failed)}. "
                f"sample_failed={failed[:5]}"
            )

        Variable.set(STATE_VAR_KEY, current_state, serialize_json=True)

        summary = {
            "source_root": SOURCE_ROOT,
            "target_root": TARGET_ROOT,
            "scanned_files": len(current_state),
            "copied_files": len(copied),
            "skipped_files": len(skipped),
            "failed_files": len(failed),
            "copied_examples": copied[:10],
            "note": "One-way sync. No delete propagation.",
        }
        print(summary)
        return summary

    wait_for_new_or_updated >> sync_changed_files()


dag = sftp_sync_dag()
