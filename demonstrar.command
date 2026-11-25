#!/bin/bash
set -e
cd "$(dirname "$0")"
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
if [ ! -x .venv/bin/python ]; then
  echo "Crie o ambiente .venv com Python 3.11+ e instale requirements.txt (veja README.md)."
  exit 1
fi
exec .venv/bin/python -m arquipelago local --nodes 3 --exit-when-done --data-dir demonstracao_execucao/dados "$@"
