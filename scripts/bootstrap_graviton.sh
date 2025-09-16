#!/usr/bin/env bash
# bootstrap_graviton.sh
# Prepara una instancia Graviton (ARM64) para correr tu stack con Docker Compose.
# /opt/iothub-stack/
#   docker-compose.yml
#   runtime/lib/*.jar
#   config/flink/*.sql
#   config/trino/* (tu catalog/config)
#   services/telematics_api/* (Dockerfile + código)

set -euo pipefail

STACK_DIR="/opt/iothub-stack"
UNIT_NAME="iothub-stack"
DOCKER_SOCK_GROUP="docker"

echo "==> [0/10] Detectando arquitectura..."
ARCH="$(uname -m)"
if [[ "$ARCH" != "aarch64" && "$ARCH" != "arm64" ]]; then
  echo "ADVERTENCIA: arquitectura $ARCH (no ARM64). Este script está optimizado para Graviton (aarch64)."
  echo "Continuando de todos modos..."
fi

echo "==> [1/10] Actualizando sistema e instalando Docker + utilidades..."
if [ -f /etc/os-release ]; then . /etc/os-release; fi

if [[ "${ID:-}" == "amzn" ]]; then
  # Amazon Linux 2023
  sudo dnf -y update
  sudo dnf -y install docker git curl unzip jq logrotate
  # Instalar Docker Compose plugin (multi-arch)
  sudo mkdir -p /usr/local/lib/docker/cli-plugins
  COMPOSE_VER="v2.39.3"
  sudo curl -L "https://github.com/docker/compose/releases/download/${COMPOSE_VER}/docker-compose-linux-$(uname -m)" \
    -o /usr/local/lib/docker/cli-plugins/docker-compose
  sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
else
  # Ubuntu/Debian fallback
  sudo apt-get update -y
  sudo apt-get install -y ca-certificates curl gnupg lsb-release git jq logrotate
  sudo install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
    https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
    | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
  sudo apt-get update -y
  sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi

echo "==> [2/10] Habilitando y arrancando Docker..."
sudo systemctl enable docker
sudo systemctl start docker

if ! id -nG "$USER" | grep -qw "$DOCKER_SOCK_GROUP"; then
  sudo usermod -aG docker "$USER" || true
  echo ">> Nota: cierra y vuelve a abrir sesión para usar 'docker' sin sudo."
fi

echo "==> [3/10] Configurando rotación nativa de logs de Docker..."
sudo mkdir -p /etc/docker
cat | sudo tee /etc/docker/daemon.json >/dev/null <<'JSON'
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "100m",
    "max-file": "7"
  }
}
JSON
sudo systemctl restart docker

echo "==> [4/10] (Opcional extra) logrotate para archivos de contenedor..."
cat | sudo tee /etc/logrotate.d/docker-containers >/dev/null <<'EOF'
/var/lib/docker/containers/*/*.log {
  rotate 7
  daily
  compress
  size 100M
  missingok
  delaycompress
  copytruncate
}
EOF
sudo logrotate -f /etc/logrotate.d/docker-containers || true

echo "==> [5/10] Ajustes de sistema..."
if ! grep -q "nofile 1048576" /etc/security/limits.conf; then
  sudo bash -c 'cat >>/etc/security/limits.conf <<LIM
* soft nofile 1048576
* hard nofile 1048576
root soft nofile 1048576
root hard nofile 1048576
LIM'
fi
if ! grep -q "vm.max_map_count" /etc/sysctl.conf; then
  echo "vm.max_map_count=262144" | sudo tee -a /etc/sysctl.conf >/dev/null
  sudo sysctl -p || true
fi

echo "==> [6/10] Verificación del directorio..."
if [ ! -d "$STACK_DIR" ]; then
  echo "ERROR: No existe ${STACK_DIR}. Copia tu proyecto allí y vuelve a correr este script."
  exit 1
fi

echo "==> [7/10] Recuerda: quita claves AWS del compose y usa IAM Role en EC2."

echo "==> [8/10] Creando servicio systemd..."
sudo tee /etc/systemd/system/${UNIT_NAME}.service >/dev/null <<EOF
[Unit]
Description=IoT Hub Stack (docker compose)
Requires=docker.service
After=docker.service

[Service]
Type=oneshot
WorkingDirectory=${STACK_DIR}
ExecStart=/usr/bin/docker compose up -d --remove-orphans
ExecStop=/usr/bin/docker compose down
RemainAfterExit=yes
TimeoutStartSec=0

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable ${UNIT_NAME}.service

echo "==> [9/10] Levantando el stack..."
sudo systemctl start ${UNIT_NAME}.service

echo "==> [10/10] Estado de contenedores:"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"