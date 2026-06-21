#!/usr/bin/env bash
# Remove the event-service systemd service. Preserves /etc/event-service and
# /var/lib/event-service by default — pass --purge to delete them as well.
#
# Usage:
#   sudo ./uninstall.sh           # remove binary + unit, keep config & data
#   sudo ./uninstall.sh --purge   # also delete config, state, user, and group
set -euo pipefail

PURGE=0
for arg in "$@"; do
    case "$arg" in
        --purge) PURGE=1 ;;
        *) echo "Unknown arg: $arg" >&2; exit 2 ;;
    esac
done

if [ "$(id -u)" -ne 0 ]; then
    echo "ERROR: uninstall.sh must be run as root (try: sudo ./uninstall.sh)" >&2
    exit 1
fi

if systemctl list-unit-files event-service.service >/dev/null 2>&1; then
    systemctl stop event-service.service || true
    systemctl disable event-service.service || true
fi
rm -f /etc/systemd/system/event-service.service
systemctl daemon-reload

rm -f /usr/local/bin/event-service

if [ "$PURGE" -eq 1 ]; then
    rm -rf /etc/event-service /var/lib/event-service
    if id -u event-service >/dev/null 2>&1; then
        userdel event-service || true
    fi
    if getent group event-service >/dev/null; then
        groupdel event-service || true
    fi
    echo "Purged config, state, user, and group."
else
    echo "Removed binary and service unit. Config preserved at /etc/event-service."
    echo "Re-run with --purge to delete config, state, user, and group."
fi
