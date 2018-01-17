#!/usr/bin/env python

"""
Runs Kibana and aws-signing-proxy locally. The latter is used to sign requests by the former and forward them to an
Amazon Elasticsearch instance. The default instance is the main ES instance for the current DSS stage.

To install Kibana and aws-signing-proxy follow these steps (macOS only):

1) Run

   brew install golang

2) As instructed by brew, set GOPATH and GOROOT, then set PATH to include $GOPATH/bin

3) Run

   go get github.com/cllunsford/aws-signing-proxy

4) Install a matching version of Kibana (currently 5.5). Finding the homebrew-core commit for that version is the
   trickiest part of the setup. It is possible that you don't need to do this as you might already have a matching
   version of Kibana installed locally. I used the following command to install Kibana 5.5.2

   brew install https://raw.githubusercontent.com/Homebrew/homebrew-core/eb26ed9e7a4b3a35c62a40fa4fec89bf0361781f/Formula/kibana.rb

To use this program, set `AWS_PROFILE` and `source environment`. Then run

   kibana-proxy.py

If multiple versions of Kibana are installed, you may want to select the one to be run by this program by setting
DSS_KIBANA_BIN (in environment.local) to point at the Kibana executable. For example, I have

   export DSS_KIBANA_BIN=/usr/local/Cellar/kibana/5.5.2/bin/kibana
"""

import logging
import os
import shlex
import signal
import sys
from itertools import chain

import boto3

log = logging.getLogger(__name__)


class KibanaProxy:

    def __init__(self, options) -> None:
        self.options = options
        self.pids = {}

    def run(self):
        kibana_port = self.options.kibana_port
        proxy_port = self.options.proxy_port or kibana_port + 10
        try:
            self.spawn('aws-signing-proxy',
                       '-target', self.dss_end_point,
                       '-port', str(proxy_port),
                       AWS_REGION=os.environ['AWS_DEFAULT_REGION'])
            self.spawn(os.environ.get('DSS_KIBANA_BIN', 'kibana'),
                       '--port', str(kibana_port),
                       '--elasticsearch', f'http://localhost:{proxy_port}')
            self.wait()
        finally:
            self.kill()

    @property
    def dss_end_point(self):
        log.info('Getting domain endpoint')
        es = boto3.client('es')
        domain = es.describe_elasticsearch_domain(DomainName=self.options.domain)
        return 'https://' + domain['DomainStatus']['Endpoint']

    def spawn(self, *args, **env):
        logged_command = ' '.join(chain(
            (k + '=' + shlex.quote(v) for k, v in env.items()),
            map(shlex.quote, args)))
        log.info('Running %s', logged_command)
        pid = os.spawnvpe(os.P_NOWAIT, args[0], args, env={**os.environ, **env})
        self.pids[pid] = logged_command

    def wait(self):
        while self.pids:
            pid, status = os.waitpid(-1, 0)
            args = self.pids.pop(pid)
            raise Exception(f'Exited: {args}')

    def kill(self):
        for pid, args in self.pids.items():
            log.info('Terminating: %s', args)
            os.kill(pid, signal.SIGINT)
            os.waitpid(pid, 0)


def main(argv):
    import argparse
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--kibana-port', '-p', metavar='PORT', default=5601, type=int,
                     help="The port Kibana should be listening on.")
    cli.add_argument('--proxy-port', '-P', metavar='PORT', type=int,
                     help="The port the proxy should be listening on. The default is the Kibana port plus 10.")
    cli.add_argument('--domain', '-d', metavar='DOMAIN', default=os.environ.get('DSS_ES_DOMAIN'),
                     help="The AWS Elasticsearch domain to use.")
    options = cli.parse_args(argv)
    if not options.domain:
        raise RuntimeError('Please pass --domain or set DSS_ES_DOMAIN')
    KibanaProxy(options).run()


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARN)
    log.setLevel(logging.INFO)
    main(sys.argv[1:])
