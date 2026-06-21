#!/usr/bin/env bash
# Install streamforge-event-service as a systemd service on Ubuntu/Debian.
# Run from inside an extracted release tarball: `sudo ./install.sh`.
#
# The tarball is expected to contain (flat, next to this script):
#   event-service               the binary
#   event-service.service       the systemd unit
#   event-service.env.example   the config template (falls back to .env.example)
set -euo pipefail

SERVICE_USER="event-service"
SERVICE_GROUP="event-service"
BIN_DIR="/usr/local/bin"
CONFIG_DIR="/etc/event-service"
STATE_DIR="/var/lib/event-service"
CONFIG_FILE="$CONFIG_DIR/event-service.env"
UNIT_PATH="/etc/systemd/system/event-service.service"

require_root() {
    if [ "$(id -u)" -ne 0 ]; then
        echo "ERROR: install.sh must be run as root (try: sudo ./install.sh)" >&2
        exit 1
    fi
}

require_systemd() {
    if ! command -v systemctl >/dev/null 2>&1; then
        echo "ERROR: systemctl not found — this installer targets systemd hosts." >&2
        exit 1
    fi
}

script_dir() {
    cd "$(dirname "$0")" && pwd
}

# Resolve the config template shipped in the archive (release name first, then
# the in-repo .env.example).
seed_source() {
    local src="$1"
    if [ -f "$src/event-service.env.example" ]; then
        echo "$src/event-service.env.example"
    elif [ -f "$src/.env.example" ]; then
        echo "$src/.env.example"
    else
        echo ""
    fi
}

main() {
    require_root
    require_systemd

    local src
    src="$(script_dir)"

    if [ ! -x "$src/event-service" ]; then
        echo "ERROR: binary $src/event-service not found or not executable." >&2
        exit 1
    fi
    if [ ! -f "$src/event-service.service" ]; then
        echo "ERROR: $src/event-service.service missing from the release archive." >&2
        exit 1
    fi

    # 1. Create system user/group (idempotent).
    if ! getent group "$SERVICE_GROUP" >/dev/null; then
        groupadd --system "$SERVICE_GROUP"
    fi
    if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
        useradd \
            --system \
            --gid "$SERVICE_GROUP" \
            --home-dir "$STATE_DIR" \
            --shell /usr/sbin/nologin \
            --comment "StreamForge Event Service" \
            "$SERVICE_USER"
    fi

    # 2. Install binary.
    install -m 0755 -o root -g root "$src/event-service" "$BIN_DIR/event-service"

    # 3. Config dir + seed config (never overwrite an existing config).
    install -d -m 0750 -o root -g "$SERVICE_GROUP" "$CONFIG_DIR"
    local seed
    seed="$(seed_source "$src")"
    if [ ! -f "$CONFIG_FILE" ]; then
        if [ -n "$seed" ]; then
            install -m 0640 -o root -g "$SERVICE_GROUP" "$seed" "$CONFIG_FILE"
            echo "Seeded $CONFIG_FILE — edit it (DATABASE_URL, REDIS_URL, Flussonic, GeoIP) before starting!"
        else
            echo "WARNING: no config template bundled; create $CONFIG_FILE manually." >&2
        fi
    else
        echo "Keeping existing $CONFIG_FILE."
        # Drop the latest template alongside it so the admin can diff new keys.
        if [ -n "$seed" ]; then
            install -m 0640 -o root -g "$SERVICE_GROUP" "$seed" "$CONFIG_DIR/event-service.env.example"
        fi
    fi

    # 4. State dir (home for the service user; default GeoIP db location).
    install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$STATE_DIR"

    # 5. Install systemd unit.
    install -m 0644 -o root -g root "$src/event-service.service" "$UNIT_PATH"

    # 6. Enable + (re)start.
    systemctl daemon-reload
    systemctl enable event-service.service
    if systemctl is-active --quiet event-service.service; then
        systemctl restart event-service.service
        echo "event-service restarted."
    else
        systemctl start event-service.service
        echo "event-service started."
    fi

    # Post-install advisories (non-fatal).
    if grep -q '^GEOIP_CITY_DB_PATH=' "$CONFIG_FILE" 2>/dev/null; then
        local geo
        geo=$(grep '^GEOIP_CITY_DB_PATH=' "$CONFIG_FILE" | head -n1 | cut -d= -f2-)
        if [ -n "$geo" ] && [ ! -f "$geo" ]; then
            echo "WARNING: GeoLite2-City DB not found at '$geo'." >&2
            echo "         Provide your own MaxMind GeoLite2-City.mmdb (license-restricted, not bundled)" >&2
            echo "         e.g. place it at $STATE_DIR/GeoLite2-City.mmdb and point GEOIP_CITY_DB_PATH at it." >&2
        fi
    fi

    cat <<EOF

Install complete.
  Binary:  $BIN_DIR/event-service
  Config:  $CONFIG_FILE
  State:   $STATE_DIR
  Service: event-service.service ($(systemctl is-enabled event-service.service))
  Status:  systemctl status event-service.service
  Logs:    journalctl -u event-service -f

After editing the config, run: systemctl restart event-service
EOF
}

main "$@"
