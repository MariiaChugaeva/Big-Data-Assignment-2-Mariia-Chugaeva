#!/usr/bin/env bash
# LF line endings only (Windows CRLF breaks bash: $'\r', mangled commands).
echo "=== app.sh: loaded (cluster-master) ==="
set -eu

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

echo "=== [app.sh 1/6] SSH for Hadoop ==="
service ssh restart || service ssh start || true

echo "=== [app.sh 2/6] HDFS + YARN (start-services.sh) ==="
bash "$ROOT/start-services.sh"

echo "=== [app.sh 3/6] venv, pip, venv-pack ==="
python3 -m venv "$ROOT/.venv"
# venv-pack requires an active venv (it checks sys.prefix). The launcher in .venv/bin
# often uses #!/usr/bin/env python3 and triggers "not a virtual environment".
# Sourcing activate here is safe: this file is created inside the container (LF), not from Windows.
# shellcheck disable=SC1090
source "$ROOT/.venv/bin/activate"
pip install --upgrade pip
pip install -r "$ROOT/requirements.txt"
rm -f "$ROOT/.venv.tar.gz"
venv-pack -o "$ROOT/.venv.tar.gz"
export PATH="$ROOT/.venv/bin:$PATH"

echo "=== [app.sh 4/6] prepare_data (writes app/data/*.txt + HDFS /input/data) ==="
bash "$ROOT/prepare_data.sh"

echo "=== [app.sh 5/6] index (MapReduce + Scylla) ==="
bash "$ROOT/index.sh"

echo "=== [app.sh 6/6] sample searches ==="
bash "$ROOT/search.sh" history science europe
bash "$ROOT/search.sh" machine learning models

echo "=== app.sh finished OK ==="
