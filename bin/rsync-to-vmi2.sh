#!/usr/bin/env bash

set -euo pipefail

rsync -avz /storage/financial-crime-world-website/build/* vmi2:/home/mw/financial-world-website/
