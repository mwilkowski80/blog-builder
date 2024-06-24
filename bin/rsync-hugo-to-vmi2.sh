#!/usr/bin/env bash

set -euox pipefail

rsync -avz /storage/hugo-website/financial-crime-world/public/* vmi2:/home/mw/financial-world-website/
