#!/bin/bash
# Repeatable deployment script -- run ON the EC2 instance (e.g. over SSH)
# after Terraform has created the instance/role/security-group.
#
# Not parameterized to one developer's username or machine -- every path
# is the documented /opt/protobuf-transfer/ layout, and the service runs
# as its own dedicated, non-root user.
#
# NOT EXECUTED in this development environment -- no EC2 instance exists
# to run it against. Review before running on a real instance.

set -euo pipefail

APP_ROOT="/opt/protobuf-transfer"
SERVICE_USER="protobuf-transfer"
JAR_SOURCE="${1:?Usage: deploy.sh <path-to-built-app-dir-or-jar> [--with-cloudwatch-agent]}"

echo "==> Installing Java (Amazon Linux / dnf assumed; adjust for your distro)"
if command -v dnf >/dev/null 2>&1; then
  sudo dnf install -y java-21-amazon-corretto-headless
elif command -v apt-get >/dev/null 2>&1; then
  sudo apt-get update -y && sudo apt-get install -y default-jdk-headless
else
  echo "Unsupported package manager -- install a JDK 21+ manually and re-run." >&2
  exit 1
fi

echo "==> Creating service user (no login, no home directory needed)"
if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
  sudo useradd --system --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
fi

echo "==> Creating directory layout under $APP_ROOT"
sudo mkdir -p "$APP_ROOT"/{app,config,data,logs,wal}
sudo chown -R "$SERVICE_USER":"$SERVICE_USER" "$APP_ROOT"

echo "==> Copying application files"
sudo cp -r "$JAR_SOURCE"/* "$APP_ROOT/app/"
sudo chown -R "$SERVICE_USER":"$SERVICE_USER" "$APP_ROOT/app"

echo "==> Installing environment config (if not already present -- never overwrite a real config)"
if [ ! -f "$APP_ROOT/config/app.env" ]; then
  sudo cp "$(dirname "$0")/app.env.template" "$APP_ROOT/config/app.env"
  echo "    Wrote a template to $APP_ROOT/config/app.env -- EDIT IT before starting the service."
fi
sudo chown "$SERVICE_USER":"$SERVICE_USER" "$APP_ROOT/config/app.env"
sudo chmod 600 "$APP_ROOT/config/app.env"

if [[ "${2:-}" == "--with-cloudwatch-agent" ]]; then
  echo "==> Installing CloudWatch Agent"
  if command -v dnf >/dev/null 2>&1; then
    sudo dnf install -y amazon-cloudwatch-agent
  else
    echo "    CloudWatch Agent package install skipped (non-Amazon-Linux) -- install manually per AWS docs." >&2
  fi
  sudo cp "$(dirname "$0")/cloudwatch-agent-config.json" /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
  sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
    -a fetch-config -m ec2 -s -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
fi

echo "==> Installing systemd service"
sudo cp "$(dirname "$0")/protobuf-transfer.service" /etc/systemd/system/protobuf-transfer.service
sudo systemctl daemon-reload
sudo systemctl enable protobuf-transfer

echo "==> Starting service"
sudo systemctl restart protobuf-transfer
sleep 2
sudo systemctl status protobuf-transfer --no-pager

echo "==> Verifying health endpoint"
source "$APP_ROOT/config/app.env"
curl -sf "http://localhost:${HTTP_PORT}/health" && echo "" && echo "Deployment OK."
