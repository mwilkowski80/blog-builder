#!/usr/bin/env bash

set -euox pipefail

rsync -avz /storage/financial-crime-world-website/build/* vmi2:/home/mw/financial-world-website/
