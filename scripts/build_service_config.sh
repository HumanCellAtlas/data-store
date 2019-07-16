#!/usr/bin/env bash

service_config_json="./chalicelib/service_config.json"
git fetch --all
export FUS_VERSION=$(eval git describe --tags --abbrev=0)
cat "./service_config.json" | jq .version=\"$FUS_VERSION\" > $service_config_json