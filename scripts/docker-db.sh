#!/usr/bin/env bash
# Start/stop database containers for local testing.
# MySQL and MariaDB run simultaneously on different host ports (MySQL=3317,
# MariaDB=3306).  The test fixture handles port selection automatically.
set -euo pipefail
cd "$(dirname "$0")/.."

case "${1:-start}" in
  start)
    echo "Starting database containers..."
    docker compose up -d --wait
    echo ""
    echo "Ready:"
    echo "  postgres  → localhost:5432  (user: postgres, db: migrateit, pass: '')"
    echo "  mysql     → localhost:3317  (user: root,   db: migrateit, pass: '')"
    echo "  mariadb   → localhost:3306  (user: root,   db: migrateit, pass: '')"
    ;;
  stop)
    echo "Stopping database containers..."
    docker compose down -v
    ;;
  restart)
    docker compose down
    exec "$0" start
    ;;
  *)
    echo "Usage: $0 {start|stop|restart}" >&2
    exit 1
    ;;
esac
