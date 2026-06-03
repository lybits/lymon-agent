#!/usr/bin/env sh
# Lymon Agent installer (Linux). Downloads the agent binary, writes its
# environment, and installs a systemd service. Driven by the one-liner the
# portal generates:
#
#   curl -fsSL https://get.lymon.io/agent | sudo bash -s -- \
#     --enroll <CODE> --enroll-url https://host/api/agent/enroll \
#     [--datasource <id>] [--modbus-host <ip>] [--modbus-port 502] [--version vX.Y.Z]
#
# The agent self-enrolls with the one-time code on first start (no permanent
# secret is written here), then streams to the ingest endpoint it receives.
set -eu

REPO="${LYMON_AGENT_REPO:-lybits/lymon-agent}"
VERSION="latest"
ENROLL_CODE=""; ENROLL_URL=""
DATASOURCE="default-source"; MODBUS_HOST=""; MODBUS_PORT="502"

while [ $# -gt 0 ]; do
  case "$1" in
    --enroll) ENROLL_CODE="$2"; shift 2 ;;
    --enroll-url) ENROLL_URL="$2"; shift 2 ;;
    --datasource) DATASOURCE="$2"; shift 2 ;;
    --modbus-host) MODBUS_HOST="$2"; shift 2 ;;
    --modbus-port) MODBUS_PORT="$2"; shift 2 ;;
    --version) VERSION="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
done

[ -n "$ENROLL_CODE" ] && [ -n "$ENROLL_URL" ] || {
  echo "error: --enroll <code> and --enroll-url <url> are required" >&2; exit 1; }

ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64) ASSET="lymon-agent-linux-x86_64" ;;
  *) echo "error: unsupported architecture '$ARCH'" >&2; exit 1 ;;
esac

if [ "$VERSION" = "latest" ]; then
  URL="https://github.com/$REPO/releases/latest/download/$ASSET"
else
  URL="https://github.com/$REPO/releases/download/$VERSION/$ASSET"
fi

echo "Downloading lymon-agent ($ASSET) …"
curl -fsSL "$URL" -o /usr/local/bin/lymon-agent
chmod +x /usr/local/bin/lymon-agent
mkdir -p /var/lib/lymon-agent

cat > /etc/lymon-agent.env <<EOF
LYMON_ENROLL_CODE=$ENROLL_CODE
LYMON_ENROLL_URL=$ENROLL_URL
LYMON_DATASOURCE_ID=$DATASOURCE
LYMON_MODBUS_HOST=${MODBUS_HOST:-CHANGE_ME}
LYMON_MODBUS_PORT=$MODBUS_PORT
LYMON_BUFFER_PATH=/var/lib/lymon-agent/buffer.db
EOF
chmod 600 /etc/lymon-agent.env

cat > /etc/systemd/system/lymon-agent.service <<'UNIT'
[Unit]
Description=Lymon Agent
After=network-online.target
Wants=network-online.target

[Service]
EnvironmentFile=/etc/lymon-agent.env
ExecStart=/usr/local/bin/lymon-agent
Restart=always
RestartSec=5
DynamicUser=yes
StateDirectory=lymon-agent

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload

if [ -n "$MODBUS_HOST" ]; then
  systemctl enable --now lymon-agent
  echo "✓ lymon-agent installed and started. Logs: journalctl -u lymon-agent -f"
else
  echo "✓ lymon-agent installed. Set your source in /etc/lymon-agent.env"
  echo "  (LYMON_MODBUS_HOST), then: sudo systemctl enable --now lymon-agent"
fi
