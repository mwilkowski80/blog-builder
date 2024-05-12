#!/usr/bin/env bash

set -euox pipefail

remote_backup_location="${REMOTE_BACKUP_LOCATION}"
bindir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
projdir="$(cd "$bindir/.." && pwd)"
backup_timestamp="$(date -u +%Y%m%d-%H%M%S)"
backup_filename="backup-${backup_timestamp}.tar.gz"

cd "$projdir"
mkdir -p ./tmp/
rm -rf "./tmp/*"
tar zcf "./tmp/${backup_filename}" "./data/"
cd ./tmp/
gpg2 --symmetric --batch --passphrase-file "$projdir/.backup_passphrase" --output "${backup_filename}.gpg" "${backup_filename}"
rclone copy "${backup_filename}.gpg" "${remote_backup_location}"
