import errno
import os
import subprocess
import sys
from pathlib import Path

__all__ = ('setenv', 'main')


def main(argv):
    import argparse
    parser = argparse.ArgumentParser(description='Install a hook into Python that automatically sources `environment`')
    parser.add_argument('action', choices=['install', 'remove'])
    options = parser.parse_args(argv)
    import site
    if hasattr(sys, 'real_prefix'):
        # virtualenv's site does not have getsitepackages()
        link_dir = os.path.abspath(os.path.join(os.path.dirname(site.__file__), 'site-packages'))
    else:
        raise RuntimeError('Need to be run from within a virtualenv')
    link = os.path.join(link_dir, 'sitecustomize.py')
    dst = os.path.abspath(__file__)
    dst = os.path.relpath(dst, link_dir)
    try:
        cur_dst = os.readlink(link)
    except FileNotFoundError:
        cur_dst = None
    except OSError as e:
        if e.errno == errno.EINVAL:
            raise RuntimeError(f"{link} is not a symbolic link. It may be a 3rd party file and we won't touch it")
        else:
            raise
    if options.action == 'install':
        if cur_dst is None:
            os.symlink(dst, link)
        elif dst == cur_dst:
            pass
        else:
            raise RuntimeError(f'{link} points somewhere unexpected ({cur_dst})')
    elif options.action == 'remove':
        if cur_dst is None:
            pass
        elif cur_dst == dst:
            os.unlink(link)
        else:
            raise RuntimeError(f'{link} points somewhere unexpected ({cur_dst})')
    else:
        assert False


def setenv():
    self = Path(__file__).resolve()
    project = self.parent.parent
    environment = project.joinpath('environment')
    before = _parse(_run('env'))
    after = _parse(_run(f'source {environment} && env'))
    diff = set(after.items()).symmetric_difference(before.items())
    for k, v in diff:
        print(f"{self.name}: Setting {k} to '{v}'", file=sys.stderr)
        os.environ[k] = v


def _run(command) -> str:
    bash = "/bin/bash"
    try:
        shell = os.environ['SHELL']
    except KeyError:
        shell = bash
    else:
        # allow a custom path to bash, but reject all other shells
        if os.path.basename(shell) != 'bash':
            shell = bash
    args = [shell, '-c', command]
    process = subprocess.run(args, stdout=subprocess.PIPE)
    output = process.stdout.decode()
    if process.returncode != 0:
        raise RuntimeError(f'Running {args} failed with {process.returncode}:\n{output}')
    return output


def _parse(env: str):
    return {k: v for k, _, v in (line.partition('=') for line in env.splitlines())}


if __name__ == '__main__':
    main(sys.argv[1:])
elif __name__ == 'sitecustomize':
    setenv()
