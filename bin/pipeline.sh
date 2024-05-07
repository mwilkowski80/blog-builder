#!/usr/bin/env bash

set -eoux pipefail

bindir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cd "$bindir"

./generate-docusaurus.sh
./rsync-to-vmi2.sh
REMOTE_BACKUP_LOCATION='mwsilenteight-backup:/blog-builder-data/' ./backup.sh