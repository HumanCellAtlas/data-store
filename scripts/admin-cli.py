#! /usr/bin/env python

"""Perform administrative tasks on the Human Cell Atlas data store"""

import argparse
import json
import sys

import boto3
import os

replicas = ['aws', 'gcp']

# This script should ultimately be moved into its own distribution such that it can be installed independently of the
# main data-store project. Specifically it should not have a code dependency on the data-store project itself.
#
# The CLI is structred as follows: There are targets (nouns like index or storage) and actions (verbs like verify or
# repair). This is easy to understand and consistent. There is one sub-command per Target subclass in
# daemons/dss-admin/app.py (the target) and one sub-sub-command per public method in that subclass (the action). The
# command line options correspond to arguments of either the constructor of the Target subclass or one of its
# methods. A command line option of a target becomes an argument to the constructor, while options of an action
# become method arguments. The type, optionality and default value of these options should reflect the signatures of
# the respective methods.


def main(args):
    options = parse_args(args)
    invoke_lambda(options)


def parse_args(args):
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--stage', default=os.environ.get('DSS_DEPLOYMENT_STAGE'))
    cli.add_argument('--workers', type=int, default=32)
    targets = cli.add_subparsers(dest='target')
    targets.required = True

    index = targets.add_parser('index')
    index.add_argument('--replica', required=True, choices=replicas)
    index.add_argument('--bucket')
    index.add_argument('--prefix')
    index_actions = index.add_subparsers(dest='action')
    index_actions.required = True
    index_actions.add_parser('verify')
    index_actions.add_parser('repair')

    storage = targets.add_parser('storage')
    storage.add_argument('--replica', dest='replicas', required=True, choices=replicas, action='append')
    for replica in replicas:
        assert '-' not in replica  # a dash would convert to
        storage.add_argument(f'--{replica}-bucket')
    storage_actions = storage.add_subparsers(dest='action')
    storage_actions.required = True
    storage_actions.add_parser('verify')

    options = cli.parse_args(args)

    if options.target == 'storage':
        options.replicas = set(options.replicas)
        if len(options.replicas) < 2:
            cli.error('Must specify at least two replicas for a consistency check')
        options.replicas = {replica: getattr(options, f'{replica}_bucket') for replica in options.replicas}

    return options


def invoke_lambda(options):
    client = boto3.client('lambda')
    request = vars(options)
    response = client.invoke(FunctionName=f'dss-admin-{options.stage}',
                             InvocationType='RequestResponse',
                             Payload=json.dumps(request))
    payload = json.loads(response['Payload'].read().decode('utf-8'))
    print(payload)


if __name__ == '__main__':
    main(sys.argv[1:])
