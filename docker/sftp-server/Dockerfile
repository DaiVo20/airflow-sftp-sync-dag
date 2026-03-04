FROM ubuntu:latest
LABEL authors="daivo"

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      openssh-server \
      ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /var/run/sshd /etc/ssh/authorized_keys

# Copy config and startup script
COPY sshd_config /etc/ssh/sshd_config
COPY entrypoint.sh /entrypoint.sh

RUN chmod +x /entrypoint.sh

EXPOSE 22

ENTRYPOINT ["/entrypoint.sh"]