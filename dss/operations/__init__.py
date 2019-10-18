"""
This file provides an interface to build the DSS operations CLI, parts of which may be
executed "in the cloud", via Lambda-SQS integration.

Commands are defined with a target-action model. While command arguments configured at the
target level are moved into the action arguments.

e.g
```
dispatch = DSSOperationsCommandDispatch()

storage = dispatch.target("storage", arguments={"--replica": dict(choices=["dev", "integration", "staging", "prod"])})

@storage.action("verify-referential-integrity")
def verify_ref_integrity(argv, args):
    ...

# execution:
scripts/dss-ops.py storage verify-referential-integrity --replica staging
```
"""
import os
import logging
import argparse
from argparse import RawTextHelpFormatter
import traceback


logger = logging.getLogger(__name__)


class _target:
    def __init__(self, target_name, dispatcher):
        self.target_name = target_name
        self.dispatcher = dispatcher

    def action(self, name: str, *, arguments: dict=None, mutually_exclusive: list=None):
        dispatcher = self.dispatcher
        arguments = arguments or dict()
        if mutually_exclusive is None:
            mutually_exclusive = dispatcher.targets[self.target_name]['mutually_exclusive'] or list()

        def register_action(obj):
            parser = dispatcher.targets[self.target_name]['subparser'].add_parser(
                name,
                help=obj.__doc__,
                formatter_class=RawTextHelpFormatter
            )
            action_arguments = dispatcher.targets[self.target_name]['arguments'].copy()
            action_arguments.update(arguments)
            for argname, kwargs in action_arguments.items():
                if argname not in mutually_exclusive:
                    parser.add_argument(argname, **(kwargs or dict()))
            if mutually_exclusive:
                group = parser.add_mutually_exclusive_group(required=True)
                for argname in mutually_exclusive:
                    kwargs = action_arguments.get(argname) or dict()
                    group.add_argument(argname, **kwargs)
            parser.set_defaults(func=obj)
            dispatcher.actions[obj] = dict(target=dispatcher.targets[self.target_name], name=name)
            return obj
        return register_action

class DSSOperationsCommandDispatch:
    """
    Grand central dispatch for DSS operations
         ___
       _(((,|    What's DNA??
      /  _-\\\\
     / C o\o \\
   _/_    __\ \\     __ __     __ __     __ __     __
  /   \ \___/  )   /--X--\   /--X--\   /--X--\   /--
  |    |\_|\  /   /--/ \--\ /--/ \--\ /--/ \--\ /--/
  |    |#  #|/          \__X__/   \__X__/   \__X__/
  (   /     |
   |  |#  # |
   |  |    #|
   |  | #___n_,_
,-/   7-' .     `\\
`-\...\-_   -  o /
   |#  # `---U--'
   `-v-^-'\/
     \  |_|_ Wny
     (___mnnm
    """
    targets: dict = dict()
    actions: dict = dict()

    def __init__(self):
        self.parser = argparse.ArgumentParser(description=self.__doc__, formatter_class=RawTextHelpFormatter)
        self.parser_targets = self.parser.add_subparsers()

    def target(self, name: str, *, arguments: dict=None, mutually_exclusive: list=None, help=None):
        arguments = arguments or dict()
        target = self.parser_targets.add_parser(name, help=help)
        self.targets[name] = dict(subparser=target.add_subparsers(),
                                  arguments=arguments,
                                  mutually_exclusive=mutually_exclusive)
        return _target(name, self)

    def __call__(self, argv):
        try:
            args = self.parser.parse_args(argv)
            action_handler = args.func(argv, args) if isinstance(args.func, type) else args.func
            try:
                action_handler(argv, args)
            except Exception:
                logger.error(traceback.format_exc())
        except SystemExit:
            pass
        except AttributeError:
            # TODO: dump error message when executed in Lambda
            self.parser.print_help()

dispatch = DSSOperationsCommandDispatch()
