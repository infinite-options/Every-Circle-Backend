#!/usr/bin/env bash
# Manual fallback: same step Zappa runs automatically via prebuild_script.
set -euo pipefail
python3 -c "from zappa_prebuild import install_lambda_crypto; install_lambda_crypto()"
