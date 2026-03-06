from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol, BinaryIO, Optional
import posixpath


# -----------------------------
# Enum-like backend selector
# -----------------------------
class TargetKind(str, Enum):
    SFTP = "sftp"
    S3 = "s3"

    @classmethod
    def parse(cls, value: str | None, default: "TargetKind" = None) -> "TargetKind":
        if default is None:
            default = cls.SFTP
        if not value:
            return default
        v = value.strip().lower()
        try:
            return cls(v)
        except ValueError:
            allowed = ", ".join([k.value for k in cls])
            raise ValueError(f"Invalid target kind {value!r}. Allowed: {allowed}")


# -----------------------------
# Config model
# -----------------------------
@dataclass(frozen=True)
class TargetConfig:
    kind: TargetKind

    # SFTP target fields
    sftp_conn_id: str
    sftp_root: str

    # S3 target fields
    aws_conn_id: str
    s3_bucket: str
    s3_prefix: str


# -----------------------------
# Unified destination reference
# -----------------------------
@dataclass(frozen=True)
class DestRef:
    """
    Canonical destination reference used for logging/observability.
    """
    kind: TargetKind
    identifier: str  # e.g. "/upload/a/b.txt" or "s3://bucket/prefix/a/b.txt"


# -----------------------------
# Protocol for target storage
# -----------------------------
class TargetStorage(Protocol):
    kind: TargetKind

    def dest_ref(self, rel_path: str) -> DestRef:
        """Return canonical dest reference for a rel_path (for logs)."""
        ...

    def ensure_parent_dir(self, rel_path: str) -> None:
        """Create parent dir/prefix if applicable (noop for S3)."""
        ...

    def put_stream(self, rel_path: str, stream: BinaryIO, size: Optional[int] = None) -> None:
        """Upload from readable stream."""
        ...

    def exists(self, rel_path: str) -> bool:
        """Optional: existence check."""
        ...


# -----------------------------
# Helpers
# -----------------------------
def _norm_rel(rel_path: str) -> str:
    # Make sure rel paths are POSIX-like and no leading slashes
    return posixpath.normpath(rel_path.lstrip("/"))


# -----------------------------
# SFTP adapter
# -----------------------------
class SFTPTargetStorage:
    kind = TargetKind.SFTP

    def __init__(self, conn_id: str, root: str):
        from airflow.providers.sftp.hooks.sftp import SFTPHook
        import stat as statmod

        self._hook = SFTPHook(ssh_conn_id=conn_id)
        self._root = posixpath.normpath(root)
        self._statmod = statmod

    def _conn(self):
        return self._hook.get_conn()

    def _path(self, rel_path: str) -> str:
        return posixpath.normpath(posixpath.join(self._root, _norm_rel(rel_path)))

    def dest_ref(self, rel_path: str) -> DestRef:
        return DestRef(kind=self.kind, identifier=self._path(rel_path))

    def ensure_parent_dir(self, rel_path: str) -> None:
        conn = self._conn()
        remote_dir = posixpath.dirname(self._path(rel_path))

        # mkdir -p
        parts = remote_dir.strip("/").split("/")
        cur = "/"
        for part in parts:
            cur = posixpath.join(cur, part)
            try:
                st = conn.stat(cur)
                if not self._statmod.S_ISDIR(st.st_mode):
                    raise RuntimeError(f"Target path exists but is not a directory: {cur}")
            except FileNotFoundError:
                conn.mkdir(cur)

    def put_stream(self, rel_path: str, stream: BinaryIO, size: Optional[int] = None) -> None:
        import os, tempfile

        conn = self._conn()
        target_path = self._path(rel_path)
        self.ensure_parent_dir(rel_path)

        # spool to temp file (disk-safe for large files)
        fd, tmp = tempfile.mkstemp(prefix="sftp_put_", suffix=".tmp")
        os.close(fd)
        try:
            with open(tmp, "wb") as f:
                while True:
                    chunk = stream.read(1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
            conn.put(tmp, target_path, confirm=True)
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)

    def exists(self, rel_path: str) -> bool:
        conn = self._conn()
        try:
            conn.stat(self._path(rel_path))
            return True
        except FileNotFoundError:
            return False


# -----------------------------
# S3 adapter
# -----------------------------
class S3TargetStorage:
    kind = TargetKind.S3

    def __init__(self, aws_conn_id: str, bucket: str, prefix: str):
        if not bucket:
            raise ValueError("S3 target requires non-empty bucket name.")
        self._bucket = bucket
        self._prefix = prefix.strip("/")

        try:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # type: ignore
        except Exception as e:
            raise RuntimeError(
                "S3 target selected but apache-airflow-providers-amazon is not installed."
            ) from e

        self._hook = S3Hook(aws_conn_id=aws_conn_id)

    def _key(self, rel_path: str) -> str:
        rel = _norm_rel(rel_path)
        return f"{self._prefix}/{rel}" if self._prefix else rel

    def dest_ref(self, rel_path: str) -> DestRef:
        return DestRef(kind=self.kind, identifier=f"s3://{self._bucket}/{self._key(rel_path)}")

    def ensure_parent_dir(self, rel_path: str) -> None:
        # No-op for S3 (prefixes are virtual)
        return

    def put_stream(self, rel_path: str, stream: BinaryIO, size: Optional[int] = None) -> None:
        import os, tempfile

        key = self._key(rel_path)

        # spool to temp file for simplicity and reproducibility
        fd, tmp = tempfile.mkstemp(prefix="s3_put_", suffix=".tmp")
        os.close(fd)
        try:
            with open(tmp, "wb") as f:
                while True:
                    chunk = stream.read(1024 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)

            self._hook.load_file(
                filename=tmp,
                key=key,
                bucket_name=self._bucket,
                replace=True,
            )
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)

    def exists(self, rel_path: str) -> bool:
        return self._hook.check_for_key(key=self._key(rel_path), bucket_name=self._bucket)


# -----------------------------
# Factory
# -----------------------------
def build_target(cfg: TargetConfig) -> TargetStorage:
    if cfg.kind == TargetKind.SFTP:
        return SFTPTargetStorage(conn_id=cfg.sftp_conn_id, root=cfg.sftp_root)
    if cfg.kind == TargetKind.S3:
        return S3TargetStorage(aws_conn_id=cfg.aws_conn_id, bucket=cfg.s3_bucket, prefix=cfg.s3_prefix)
    raise ValueError(f"Unsupported target kind: {cfg.kind}")
