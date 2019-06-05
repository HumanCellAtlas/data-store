#!/usr/bin/env bash
# default entry point for monitoring scripts

git clone https://github.com/HumanCellAtlas/data-store.git
cd data-store
git fetch
git checkout amar-monitorLiz
virtualenv venv
source venv/bin/activate
pip install boto3 requests
source environment
python3 scripts/monitor_lambdas.py
