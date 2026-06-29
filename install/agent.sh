#!/usr/bin/env sh
# Lymon Agent installer (Linux + macOS). Downloads the agent bundle, writes its
# environment, and installs a service (systemd on Linux, launchd on macOS).
# Driven by the one-liner the portal generates:
#
#   curl -fsSL https://host/install/agent.sh | sudo bash -s -- \
#     --enroll <CODE> --enroll-url https://host/api/agent/enroll \
#     [--datasource <id>] [--modbus-host <ip>] [--modbus-port 502] [--version vX.Y.Z]
#
# The agent self-enrolls with the one-time code on first start (no permanent
# secret is written here), then streams to the ingest endpoint it receives.
set -eu

err() { echo "error: $*" >&2; exit 1; }

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
    *) err "unknown argument: $1" ;;
  esac
done

[ -n "$ENROLL_CODE" ] && [ -n "$ENROLL_URL" ] || \
  err "--enroll <code> and --enroll-url <url> are required"
[ "$(id -u)" = "0" ] || err "run with sudo (the installer writes a system service)"

# ── Platform detection ───────────────────────────────────────────────────
# Released bundles: linux-x86_64, macos-arm64, windows-x86_64. We map (OS,arch)
# to the matching slug and fail clearly on combos with no published build.
OS="$(uname -s)"
ARCH="$(uname -m)"
case "$OS" in
  Linux)
    case "$ARCH" in
      x86_64|amd64) SLUG="linux-x86_64" ;;
      *) err "no Linux build for arch '$ARCH' (only x86_64)" ;;
    esac ;;
  Darwin)
    case "$ARCH" in
      arm64|aarch64) SLUG="macos-arm64" ;;
      *) err "macOS build is Apple Silicon only (arch '$ARCH' has no build)" ;;
    esac ;;
  *) err "unsupported OS '$OS'" ;;
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

# Per-OS install locations. Binary + plugins are shared paths; state differs.
BIN=/usr/local/bin/lymon-agent
PLUGINS_DIR=/usr/local/lib/lymon-agent/plugins
ENV_FILE=/etc/lymon-agent.env
if [ "$OS" = "Darwin" ]; then
  STATE_DIR=/usr/local/var/lymon-agent
  LOG=/usr/local/var/log/lymon-agent.log
else
  STATE_DIR=/var/lib/lymon-agent
  LOG=/var/log/lymon-agent.log
fi

echo "Downloading lymon-agent bundle ($ASSET) …"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
curl -fsSL "$URL" -o "$TMP/bundle.tar.gz" || err "download failed: $URL"
tar -xzf "$TMP/bundle.tar.gz" -C "$TMP"

install -d -m 0755 /usr/local/bin
install -m 0755 "$TMP/$BUNDLE/lymon-agent" "$BIN"
install -d -m 0755 "$STATE_DIR"
rm -rf "$PLUGINS_DIR"
mkdir -p "$PLUGINS_DIR"
cp -R "$TMP/$BUNDLE/plugins/." "$PLUGINS_DIR/"
chmod -R a+rX "$PLUGINS_DIR"
# Plugin executables (everything that isn't a manifest) need the exec bit.
find "$PLUGINS_DIR" -type f ! -name '*.json' -exec chmod a+x {} +

# macOS: a curl-downloaded binary usually isn't quarantined, but strip the
# quarantine xattr defensively so Gatekeeper never blocks the agent/plugins.
if [ "$OS" = "Darwin" ]; then
  xattr -dr com.apple.quarantine "$BIN" "$PLUGINS_DIR" 2>/dev/null || true
fi

cat > "$ENV_FILE" <<EOF
LYMON_ENROLL_CODE=$ENROLL_CODE
LYMON_ENROLL_URL=$ENROLL_URL
LYMON_DATASOURCE_ID=$DATASOURCE
LYMON_MODBUS_HOST=${MODBUS_HOST:-CHANGE_ME}
LYMON_MODBUS_PORT=$MODBUS_PORT
LYMON_BUFFER_PATH=$STATE_DIR/buffer.db
LYMON_PLUGINS_DIR=$PLUGINS_DIR
EOF
chmod 600 "$ENV_FILE"

# ── Self-update consumer (shared) ────────────────────────────────────────
# The agent can't replace its own running binary; on a cloud "update" command
# it drops a trigger file. A scheduled root task (systemd timer / launchd
# StartInterval) polls it, swaps the WHOLE bundle (binary + plugins), restarts.
install -d -m 0755 /usr/local/lib/lymon-agent
cat > /usr/local/lib/lymon-agent/update.sh <<'UPD'
#!/usr/bin/env sh
set -eu
TRIGGER_DIR_LINUX=/var/lib/lymon-agent
TRIGGER_DIR_MAC=/usr/local/var/lymon-agent
OS="$(uname -s)"; [ "$OS" = "Darwin" ] && TRIGGER="$TRIGGER_DIR_MAC/update-request.json" || TRIGGER="$TRIGGER_DIR_LINUX/update-request.json"
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
case "$OS" in
  Darwin) case "$(uname -m)" in arm64|aarch64) SLUG="macos-arm64" ;; *) echo "unsupported mac arch" >&2; rm -f "$TRIGGER"; exit 1 ;; esac ;;
  Linux)  case "$(uname -m)" in x86_64|amd64) SLUG="linux-x86_64" ;; *) echo "unsupported linux arch" >&2; rm -f "$TRIGGER"; exit 1 ;; esac ;;
  *) echo "unsupported os" >&2; rm -f "$TRIGGER"; exit 1 ;;
esac
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
[ "$OS" = "Darwin" ] && xattr -dr com.apple.quarantine /usr/local/bin/lymon-agent "$PLUGINS_DIR" 2>/dev/null || true
rm -f "$TRIGGER"
if [ "$OS" = "Darwin" ]; then
  launchctl kickstart -k system/es.lybits.lymon-agent 2>/dev/null || true
else
  systemctl restart lymon-agent
fi
echo "lymon-agent updated to $TAG"
UPD
chmod 0755 /usr/local/lib/lymon-agent/update.sh

# ── Service setup (per OS) ───────────────────────────────────────────────
if [ "$OS" = "Darwin" ]; then
  install -d -m 0755 "$(dirname "$LOG")"
  AGENT_PLIST=/Library/LaunchDaemons/es.lybits.lymon-agent.plist
  UPDATE_PLIST=/Library/LaunchDaemons/es.lybits.lymon-agent-update.plist

  # launchd has no EnvironmentFile; run a tiny wrapper that sources the env
  # file (keeps it editable like the Linux flow) and exec's the agent.
  cat > "$AGENT_PLIST" <<UNIT
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>es.lybits.lymon-agent</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/sh</string>
    <string>-c</string>
    <string>set -a; . $ENV_FILE; set +a; exec $BIN</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>$LOG</string>
  <key>StandardErrorPath</key><string>$LOG</string>
</dict>
</plist>
UNIT

  cat > "$UPDATE_PLIST" <<UNIT
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>es.lybits.lymon-agent-update</string>
  <key>ProgramArguments</key>
  <array><string>/usr/local/lib/lymon-agent/update.sh</string></array>
  <key>StartInterval</key><integer>60</integer>
  <key>RunAtLoad</key><true/>
</dict>
</plist>
UNIT

  # (Re)load the update poller, and the agent itself once a source is set.
  launchctl bootout system "$UPDATE_PLIST" 2>/dev/null || launchctl unload "$UPDATE_PLIST" 2>/dev/null || true
  launchctl bootstrap system "$UPDATE_PLIST" 2>/dev/null || launchctl load -w "$UPDATE_PLIST"

  launchctl bootout system "$AGENT_PLIST" 2>/dev/null || launchctl unload "$AGENT_PLIST" 2>/dev/null || true
  if [ -n "$MODBUS_HOST" ]; then
    launchctl bootstrap system "$AGENT_PLIST" 2>/dev/null || launchctl load -w "$AGENT_PLIST"
    echo "✓ lymon-agent installed and started. Logs: tail -f $LOG"
  else
    echo "✓ lymon-agent installed. Set your source in $ENV_FILE (LYMON_MODBUS_HOST),"
    echo "  then: sudo launchctl bootstrap system $AGENT_PLIST"
  fi
  exit 0
fi

# ── Linux (systemd) ──────────────────────────────────────────────────────
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
