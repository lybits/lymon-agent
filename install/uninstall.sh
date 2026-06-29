#!/usr/bin/env sh
# Lymon Agent uninstaller (Linux + macOS) — reverses agent.sh: stops and removes
# the service (systemd / launchd), the self-update poller, the binary, the env
# file, the plugins tree and the agent state. Driven by the portal one-liner:
#
#   curl -fsSL https://host/install/uninstall.sh | sudo sh
#
# Pass --keep-data to leave the buffer/state directory in place.
set -eu

KEEP_DATA=0
while [ $# -gt 0 ]; do
  case "$1" in
    --keep-data) KEEP_DATA=1; shift ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
done

[ "$(id -u)" = "0" ] || { echo "error: run as root (sudo)" >&2; exit 1; }

OS="$(uname -s)"

if [ "$OS" = "Darwin" ]; then
  # ── macOS (launchd) ──────────────────────────────────────────────────────
  for label in es.lybits.lymon-agent es.lybits.lymon-agent-update; do
    plist="/Library/LaunchDaemons/$label.plist"
    launchctl bootout "system/$label" 2>/dev/null || launchctl unload "$plist" 2>/dev/null || true
    rm -f "$plist"
  done
  rm -f /usr/local/bin/lymon-agent /etc/lymon-agent.env
  rm -rf /usr/local/lib/lymon-agent
  if [ "$KEEP_DATA" = "1" ]; then
    echo "Kept /usr/local/var/lymon-agent — remove it manually to wipe the buffer."
  else
    rm -rf /usr/local/var/lymon-agent
  fi
  echo "✓ lymon-agent uninstalled."
  exit 0
fi

# ── Linux (systemd) ────────────────────────────────────────────────────────
# Stop + disable units (service + self-update timer).
for unit in lymon-agent.service lymon-agent-update.timer lymon-agent-update.service; do
  systemctl disable --now "$unit" 2>/dev/null || true
done

rm -f /etc/systemd/system/lymon-agent.service \
      /etc/systemd/system/lymon-agent-update.service \
      /etc/systemd/system/lymon-agent-update.timer
systemctl daemon-reload 2>/dev/null || true
systemctl reset-failed lymon-agent.service 2>/dev/null || true

rm -f /usr/local/bin/lymon-agent /etc/lymon-agent.env
rm -rf /usr/local/lib/lymon-agent

if [ "$KEEP_DATA" = "1" ]; then
  echo "Kept /var/lib/lymon-agent — remove it manually to wipe the buffer."
else
  rm -rf /var/lib/lymon-agent
fi

echo "✓ lymon-agent uninstalled."
