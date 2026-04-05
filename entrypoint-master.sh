#!/usr/bin/env bash
# Strip CRLF from /app/*.sh (Windows); sed -i is unreliable on Docker Desktop bind mounts.
echo ""
echo "=== entrypoint-master.sh: normalizing /app/*.sh (CRLF -> LF) ==="
set -eu
python3 << 'PY'
import pathlib
for p in pathlib.Path("/app").glob("*.sh"):
    b = p.read_bytes()
    if b"\r" in b:
        p.write_bytes(b.replace(b"\r\n", b"\n").replace(b"\r", b"\n"))
        print("fixed:", p)
PY
echo "=== entrypoint-master.sh: starting /app/app.sh ==="
exec bash /app/app.sh
