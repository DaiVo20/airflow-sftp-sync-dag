#!/usr/bin/env bash

# Runtime env vars (override in docker-compose)
SFTP_USERNAME="${SFTP_USERNAME:-sftpuser}"
SFTP_PASSWORD="${SFTP_PASSWORD:-sftppass}"
SFTP_UID="${SFTP_UID:-1001}"
SFTP_GID="${SFTP_GID:-1001}"

# Chroot base and writable upload dir
CHROOT_BASE="/sftp/${SFTP_USERNAME}"
UPLOAD_DIR="${CHROOT_BASE}/upload"

# Create group/user if not exists
if ! getent group sftpusers >/dev/null; then
  groupadd -r sftpusers
fi

if ! getent group "${SFTP_GID}" >/dev/null; then
  groupadd -g "${SFTP_GID}" "sftpgrp_${SFTP_GID}" || true
fi

if ! id -u "${SFTP_USERNAME}" >/dev/null 2>&1; then
  useradd -u "${SFTP_UID}" -g sftpusers -M -d "/upload" -s /usr/sbin/nologin "${SFTP_USERNAME}"
fi

echo "${SFTP_USERNAME}:${SFTP_PASSWORD}" | chpasswd

# Chroot dir must be owned by root and not writable by user
mkdir -p "${UPLOAD_DIR}"
chown root:root "${CHROOT_BASE}"
chmod 755 "${CHROOT_BASE}"

# Writable subdir for user uploads/files
chown -R "${SFTP_USERNAME}:sftpusers" "${UPLOAD_DIR}"
chmod 755 "${UPLOAD_DIR}"

# Optional authorized key support (mount public key to /run/keys/<user>.pub)
if [[ -f "/run/keys/${SFTP_USERNAME}.pub" ]]; then
  mkdir -p "/home/${SFTP_USERNAME}/.ssh"
  cp "/run/keys/${SFTP_USERNAME}.pub" "/home/${SFTP_USERNAME}/.ssh/authorized_keys"
  chown -R "${SFTP_USERNAME}:sftpusers" "/home/${SFTP_USERNAME}/.ssh"
  chmod 700 "/home/${SFTP_USERNAME}/.ssh"
  chmod 600 "/home/${SFTP_USERNAME}/.ssh/authorized_keys"
fi

# Generate host keys on first start
ssh-keygen -A

exec /usr/sbin/sshd -D -e