#!/usr/bin/env bash

set -euox pipefail

bindir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
projdir="$(cd "$bindir/.." && pwd)"

export TERM=dumb
cd "$projdir"
source venv/bin/activate

./venv/bin/python -m blogbuilder.main generate-docusaurus-articles \
  --output-dir /storage/financial-crime-world-website/blog \
  --markdown-articles-dir ./data/markdown-articles \
  --authors-yml-file /storage/financial-crime-world-website/blog/authors.yml \
  --operation-log-path "$projdir"/data/generate-docusaurus-operation-log.json \
  --skip-existing
