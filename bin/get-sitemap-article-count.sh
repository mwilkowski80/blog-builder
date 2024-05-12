#!/usr/bin/env bash

set -euox pipefail
curl -s https://financialcrime.world/sitemap.xml | xmllint -format - | grep '<loc>' | wc -l
