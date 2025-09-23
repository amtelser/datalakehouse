#!/usr/bin/env bash
# bootstrap_graviton.sh v2 anti-hang
# Prepara una instancia Graviton (ARM64) para correr tu stack con Docker Compose.
set -euo pipefail

STACK_DIR="/opt/iothub-stack"
UNIT_NAME="iothub-stack"
DOCKER_SOCK_GROUP="docker"
COMPOSE_VER="v2.39.3"

echo "==> [0/10] Detectando arquitectura..."
ARCH="$(uname -m)"
if [[ "$ARCH" != "aarch64" && "$ARCH" != "arm64" ]]; then
  echo "ADVERTENCIA: arquitectura $ARCH (no ARM64). Optimizado para Graviton."
fi

echo "==> [1/10] Actualizando sistema e instalando Docker + utilidades..."
if [ -f /etc/os-release ]; then . /etc/os-release; fi
if [[ "${ID:-}" == "amzn" ]]; then
  # Amazon Linux 2023
  sudo dnf -y update
  sudo dnf -y install docker git curl unzip jq logrotate
  sudo mkdir -p /usr/local/lib/docker/cli-plugins
  sudo curl -L "https://github.com/docker/compose/releases/download/${COMPOSE_VER}/docker-compose-linux-$(uname -m)" \
    -o /usr/local/lib/docker/cli-plugins/docker-compose
  sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
else
  # Ubuntu/Debian
  sudo apt-get update -y
  sudo apt-get install -y ca-certificates curl gnupg lsb-release git jq logrotate
  sudo install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
    | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
  sudo apt-get update -y
  sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi

echo "==> [2/10] Habilitando y arrancando Docker..."
sudo systemctl enable docker
sudo systemctl start docker
if ! id -nG "$USER" | grep -qw "$DOCKER_SOCK_GROUP"; then
  sudo usermod -aG docker "$USER" || true
  echo ">> Nota: re-login para usar 'docker' sin sudo."
fi

echo "==> [3/10] Configurando rotación nativa de logs de Docker (robusto)..."
sudo mkdir -p /etc/docker
sudo tee /etc/docker/daemon.json >/dev/null <<'JSON'
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "100m",
    "max-file": "7"
  }
}
JSON
sudo chown root:root /etc/docker/daemon.json
sudo chmod 644 /etc/docker/daemon.json

# Validar JSON antes de tocar Docker
if ! sudo jq . /etc/docker/daemon.json >/dev/null 2>&1; then
  echo "ERROR: /etc/docker/daemon.json inválido."; exit 1
fi

# Reload + restart no-bloqueante
sudo systemctl daemon-reload || true
sudo systemctl reload docker || true
sudo systemctl restart docker --no-block || true
sleep 3

if ! systemctl is-active --quiet docker; then
  echo "Docker no activo tras restart no-bloqueante; mostrando logs:"
  sudo journalctl -u docker -n 80 --no-pager || true
  echo "Reintentando restart síncrono..."
  sudo systemctl restart docker || true
  sleep 2
  systemctl is-active --quiet docker || { echo "Docker sigue inactivo. Abortando."; exit 1; }
fi
echo "Docker activo 👍"

echo "==> [4/10] (Opcional) logrotate para archivos de contenedor (sin forzar)..."
# Heredoc robusto (sin cat | tee) y sin -f que puede tardar si hay logs grandes
sudo bash -c 'cat > /etc/logrotate.d/docker-containers << "EOF"
/var/lib/docker/containers/*/*.log {
  rotate 7
  daily
  compress
  size 100M
  missingok
  delaycompress
  copytruncate
}
EOF'
sudo chown root:root /etc/logrotate.d/docker-containers
sudo chmod 644 /etc/logrotate.d/docker-containers
# Validar configuración (debug: no rota nada)
sudo logrotate -d /etc/logrotate.d/docker-containers || true

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
[ -d "$STACK_DIR" ] || { echo "ERROR: Falta ${STACK_DIR}. Copia tu proyecto allí y vuelve a correr este script."; exit 1; }

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