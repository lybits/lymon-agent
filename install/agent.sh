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
  x86_64|amd64) SLUG="linux-x86_64" ;;
  *) echo "error: unsupported architecture '$ARCH'" >&2; exit 1 ;;
esac
# The release ships a drop-in BUNDLE per platform: the agent binary plus a
# plugins/ tree (opcua, s7, …) in the layout PluginHost::discover expects.
BUNDLE="lymon-agent-$SLUG"
ASSET="$BUNDLE.tar.gz"

if [ "$VERSION" = "latest" ]; then
  URL="https://github.com/$REPO/releases/latest/download/$ASSET"
else
  URL="https://github.com/$REPO/releases/download/$VERSION/$ASSET"
fi

echo "Downloading lymon-agent bundle ($ASSET) …"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
curl -fsSL "$URL" -o "$TMP/bundle.tar.gz"
tar -xzf "$TMP/bundle.tar.gz" -C "$TMP"

install -m 0755 "$TMP/$BUNDLE/lymon-agent" /usr/local/bin/lymon-agent
mkdir -p /var/lib/lymon-agent
# Plugins live in a stable, world-readable location (NOT the StateDirectory,
# which systemd remaps under DynamicUser): the agent reads them via
# LYMON_PLUGINS_DIR set in the env file below.
PLUGINS_DIR=/usr/local/lib/lymon-agent/plugins
rm -rf "$PLUGINS_DIR"
mkdir -p "$PLUGINS_DIR"
cp -R "$TMP/$BUNDLE/plugins/." "$PLUGINS_DIR/"
chmod -R a+rX "$PLUGINS_DIR"
# Plugin executables (everything that isn't a manifest) need the exec bit.
find "$PLUGINS_DIR" -type f ! -name '*.json' -exec chmod a+x {} +

cat > /etc/lymon-agent.env <<EOF
LYMON_ENROLL_CODE=$ENROLL_CODE
LYMON_ENROLL_URL=$ENROLL_URL
LYMON_DATASOURCE_ID=$DATASOURCE
LYMON_MODBUS_HOST=${MODBUS_HOST:-CHANGE_ME}
LYMON_MODBUS_PORT=$MODBUS_PORT
LYMON_BUFFER_PATH=/var/lib/lymon-agent/buffer.db
LYMON_PLUGINS_DIR=$PLUGINS_DIR
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

# ── Self-update (phase 2) ────────────────────────────────────────────────
# The agent (DynamicUser) can't replace its own binary; on a cloud "update"
# command it drops a trigger file in its StateDirectory. A ROOT timer polls
# that trigger and swaps the WHOLE bundle (binary + plugins) then restarts.
install -d -m 0755 /usr/local/lib/lymon-agent
cat > /usr/local/lib/lymon-agent/update.sh <<'UPD'
#!/usr/bin/env sh
# Consume the agent's update-request trigger: download the requested bundle,
# swap binary + plugins, restart. Idempotent; a no-op when there's no trigger.
set -eu
TRIGGER=/var/lib/lymon-agent/update-request.json
[ -f "$TRIGGER" ] || exit 0
REPO_DEFAULT="lybits/lymon-agent"
VERSION=$(sed -n 's/.*"version"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$TRIGGER")
REPO=$(sed -n 's/.*"repo"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' "$TRIGGER")
[ -n "$REPO" ] || REPO="$REPO_DEFAULT"
# Trigger is attacker-relevant (feeds a URL); re-validate even though the agent did.
if [ -z "$VERSION" ] || printf '%s' "$VERSION" | grep -q '[^0-9A-Za-z._v-]'; then
  echo "lymon-agent update: bad version '$VERSION'; dropping trigger" >&2; rm -f "$TRIGGER"; exit 1
fi
if printf '%s' "$REPO" | grep -q '[^0-9A-Za-z._/-]'; then
  echo "lymon-agent update: bad repo '$REPO'; dropping trigger" >&2; rm -f "$TRIGGER"; exit 1
fi
ARCH="$(uname -m)"
case "$ARCH" in x86_64|amd64) SLUG="linux-x86_64" ;; *) echo "unsupported arch $ARCH" >&2; rm -f "$TRIGGER"; exit 1 ;; esac
BUNDLE="lymon-agent-$SLUG"
TAG="$VERSION"; case "$TAG" in v*) : ;; *) TAG="v$TAG" ;; esac   # release tags are vX.Y.Z
URL="https://github.com/$REPO/releases/download/$TAG/$BUNDLE.tar.gz"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
echo "lymon-agent update: fetching $URL"
curl -fsSL "$URL" -o "$TMP/b.tar.gz"
tar -xzf "$TMP/b.tar.gz" -C "$TMP"
install -m 0755 "$TMP/$BUNDLE/lymon-agent" /usr/local/bin/lymon-agent
PLUGINS_DIR=/usr/local/lib/lymon-agent/plugins
rm -rf "$PLUGINS_DIR"; mkdir -p "$PLUGINS_DIR"
cp -R "$TMP/$BUNDLE/plugins/." "$PLUGINS_DIR/"
chmod -R a+rX "$PLUGINS_DIR"
find "$PLUGINS_DIR" -type f ! -name '*.json' -exec chmod a+x {} +
rm -f "$TRIGGER"
systemctl restart lymon-agent
echo "lymon-agent updated to $TAG"
UPD
chmod 0755 /usr/local/lib/lymon-agent/update.sh

cat > /etc/systemd/system/lymon-agent-update.service <<'UNIT'
[Unit]
Description=Lymon Agent self-update (consume update-request trigger)
[Service]
Type=oneshot
ExecStart=/usr/local/lib/lymon-agent/update.sh
UNIT

cat > /etc/systemd/system/lymon-agent-update.timer <<'UNIT'
[Unit]
Description=Poll for Lymon Agent update requests
[Timer]
OnBootSec=60
OnUnitActiveSec=60
[Install]
WantedBy=timers.target
UNIT

systemctl daemon-reload
systemctl enable --now lymon-agent-update.timer

if [ -n "$MODBUS_HOST" ]; then
  systemctl enable --now lymon-agent
  echo "✓ lymon-agent installed and started. Logs: journalctl -u lymon-agent -f"
else
  echo "✓ lymon-agent installed. Set your source in /etc/lymon-agent.env"
  echo "  (LYMON_MODBUS_HOST), then: sudo systemctl enable --now lymon-agent"
fi
