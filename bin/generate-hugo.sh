#!/usr/bin/env bash

set -euox pipefail

bindir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
projdir="$(cd "$bindir/.." && pwd)"

export TERM=dumb
cd "$projdir"
source venv/bin/activate

./venv/bin/python -m blogbuilder.main generate-hugo-articles \
  --output-dir /media/mw/Storage/hugo-website/financial-crime-world/ \
  --markdown-articles-dir ./data/markdown-articles \
  --authors-yml-file /storage/financial-crime-world-website/blog/authors.yml
