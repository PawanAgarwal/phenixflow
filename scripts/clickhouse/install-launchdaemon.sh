#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CLICKHOUSE_DAEMON_LABEL="${CLICKHOUSE_DAEMON_LABEL:-com.phenixflow.clickhouse}"
CLICKHOUSE_DAEMON_USER="${CLICKHOUSE_DAEMON_USER:-${SUDO_USER:-$(/usr/bin/stat -f %Su /dev/console 2>/dev/null || /usr/bin/id -un)}}"
CLICKHOUSE_DAEMON_GROUP="${CLICKHOUSE_DAEMON_GROUP:-staff}"
CLICKHOUSE_SYSTEM_DIR="${CLICKHOUSE_SYSTEM_DIR:-/Library/Application Support/PhenixFlow/clickhouse}"
CLICKHOUSE_DAEMON_PLIST="${CLICKHOUSE_DAEMON_PLIST:-/Library/LaunchDaemons/${CLICKHOUSE_DAEMON_LABEL}.plist}"
CLICKHOUSE_OUT_LOG="${CLICKHOUSE_OUT_LOG:-/Library/Logs/phenixflow-clickhouse.launchd.out.log}"
CLICKHOUSE_ERR_LOG="${CLICKHOUSE_ERR_LOG:-/Library/Logs/phenixflow-clickhouse.launchd.err.log}"
CLICKHOUSE_USERS_SOURCE="${CLICKHOUSE_USERS_SOURCE:-$SCRIPT_DIR/../../config/clickhouse/users.xml}"
CLICKHOUSE_LAUNCHD_BOOTSTRAP="${CLICKHOUSE_LAUNCHD_BOOTSTRAP:-1}"

if [[ ! -f "$SCRIPT_DIR/launch-clickhouse.sh" ]]; then
  echo "Missing launch script: $SCRIPT_DIR/launch-clickhouse.sh" >&2
  exit 1
fi

if [[ ! -f "$CLICKHOUSE_USERS_SOURCE" ]]; then
  echo "Missing ClickHouse users config source: $CLICKHOUSE_USERS_SOURCE" >&2
  exit 1
fi

if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
  exec sudo -E bash "$0" "$@"
fi

GUI_UID="$(
  /usr/bin/id -u "$CLICKHOUSE_DAEMON_USER" 2>/dev/null || true
)"
USER_AGENT_PLIST="/Users/$CLICKHOUSE_DAEMON_USER/Library/LaunchAgents/${CLICKHOUSE_DAEMON_LABEL}.plist"

/usr/bin/install -d -m 755 "/Library/Application Support/PhenixFlow"
/usr/bin/install -d -m 755 "$CLICKHOUSE_SYSTEM_DIR"
/usr/bin/install -d -m 755 "/Library/Logs"

/usr/bin/install -m 755 "$SCRIPT_DIR/launch-clickhouse.sh" "$CLICKHOUSE_SYSTEM_DIR/launch-clickhouse.sh"
/usr/bin/install -m 644 "$CLICKHOUSE_USERS_SOURCE" "$CLICKHOUSE_SYSTEM_DIR/users.xml"

/usr/bin/touch "$CLICKHOUSE_OUT_LOG" "$CLICKHOUSE_ERR_LOG"
/usr/sbin/chown "$CLICKHOUSE_DAEMON_USER:$CLICKHOUSE_DAEMON_GROUP" "$CLICKHOUSE_SYSTEM_DIR"
/usr/sbin/chown "$CLICKHOUSE_DAEMON_USER:$CLICKHOUSE_DAEMON_GROUP" "$CLICKHOUSE_SYSTEM_DIR/launch-clickhouse.sh" "$CLICKHOUSE_SYSTEM_DIR/users.xml"
/usr/sbin/chown "$CLICKHOUSE_DAEMON_USER:$CLICKHOUSE_DAEMON_GROUP" "$CLICKHOUSE_OUT_LOG" "$CLICKHOUSE_ERR_LOG"
/bin/chmod 755 "$CLICKHOUSE_SYSTEM_DIR" "$CLICKHOUSE_SYSTEM_DIR/launch-clickhouse.sh"
/bin/chmod 644 "$CLICKHOUSE_SYSTEM_DIR/users.xml" "$CLICKHOUSE_OUT_LOG" "$CLICKHOUSE_ERR_LOG"

/bin/cat > "$CLICKHOUSE_DAEMON_PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>${CLICKHOUSE_DAEMON_LABEL}</string>

  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>${CLICKHOUSE_SYSTEM_DIR}/launch-clickhouse.sh</string>
  </array>

  <key>WorkingDirectory</key>
  <string>${CLICKHOUSE_SYSTEM_DIR}</string>

  <key>UserName</key>
  <string>${CLICKHOUSE_DAEMON_USER}</string>

  <key>GroupName</key>
  <string>${CLICKHOUSE_DAEMON_GROUP}</string>

  <key>RunAtLoad</key>
  <true/>

  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key>
    <false/>
  </dict>

  <key>ThrottleInterval</key>
  <integer>30</integer>

  <key>ProcessType</key>
  <string>Background</string>

  <key>StandardOutPath</key>
  <string>${CLICKHOUSE_OUT_LOG}</string>

  <key>StandardErrorPath</key>
  <string>${CLICKHOUSE_ERR_LOG}</string>
</dict>
</plist>
EOF

/usr/sbin/chown root:wheel "$CLICKHOUSE_DAEMON_PLIST"
/bin/chmod 644 "$CLICKHOUSE_DAEMON_PLIST"

if [[ -n "$GUI_UID" && -f "$USER_AGENT_PLIST" ]]; then
  /bin/launchctl bootout "gui/${GUI_UID}" "$USER_AGENT_PLIST" >/dev/null 2>&1 || true
fi
if [[ -n "$GUI_UID" ]]; then
  /bin/launchctl disable "gui/${GUI_UID}/${CLICKHOUSE_DAEMON_LABEL}" >/dev/null 2>&1 || true
fi

if [[ "$CLICKHOUSE_LAUNCHD_BOOTSTRAP" == "1" ]]; then
  /bin/launchctl bootout system "$CLICKHOUSE_DAEMON_PLIST" >/dev/null 2>&1 || true
  /bin/launchctl bootstrap system "$CLICKHOUSE_DAEMON_PLIST"
  /bin/launchctl enable "system/${CLICKHOUSE_DAEMON_LABEL}"
  /bin/launchctl kickstart -k "system/${CLICKHOUSE_DAEMON_LABEL}"
fi

echo "Installed LaunchDaemon: $CLICKHOUSE_DAEMON_PLIST"
echo "Launcher directory: $CLICKHOUSE_SYSTEM_DIR"
echo "Runs as: $CLICKHOUSE_DAEMON_USER:$CLICKHOUSE_DAEMON_GROUP"
if [[ -n "$GUI_UID" ]]; then
  echo "Disabled per-user LaunchAgent label for uid $GUI_UID: $CLICKHOUSE_DAEMON_LABEL"
fi
