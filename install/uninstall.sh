#!/usr/bin/env sh
# Lymon Agent uninstaller (Linux). Reverses install.sh: stops + disables the
# systemd service, removes the unit, the binary, the env file and the state
# directory (the local durable buffer). Driven by the one-liner the portal
# generates:
#
#   curl -fsSL https://get.lymon.io/uninstall.sh | sudo sh
#
# Removing the agent here does NOT remove it from the Lymon portal — delete it
# from the Agents page so it stops showing as enrolled/offline.
set -eu

UNIT="lymon-agent"

if command -v systemctl >/dev/null 2>&1; then
  # `|| true` so a half-installed/already-removed agent still cleans up the rest.
  systemctl disable --now "$UNIT" 2>/dev/null || true
fi
rm -f "/etc/systemd/system/${UNIT}.service"
if command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload 2>/dev/null || true
fi

rm -f /usr/local/bin/lymon-agent
rm -f /etc/lymon-agent.env
rm -rf /var/lib/lymon-agent

echo "✓ lymon-agent uninstalled (service, binary, config and local buffer removed)."
echo "  Remember to delete the agent from the Lymon portal → Agents."
