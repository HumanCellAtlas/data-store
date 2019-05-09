#!/usr/bin/env bash
# TODO make this description not suck
# This script is used to generate a requirements layer for chalice to use

pip install --install-option="--prefix=$PREFIX_PATH" package_name
