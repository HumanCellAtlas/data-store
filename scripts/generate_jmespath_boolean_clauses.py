#! /usr/bin/env python

import os
import sys
import argparse
from random import randint
import jmespath
from jmespath.exceptions import JMESPathError

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import dss
from dss import Config, Replica
from dss.events.handlers.notify_v2 import build_bundle_metadata_document, should_notify


Config.set_config(dss.BucketConfig.NORMAL)


def _jmespath_paths(obj, keypath_prefix=tuple()):
    paths = list()

    if isinstance(obj, dict):
        for k in obj.keys():
            paths += _jmespath_paths(obj[k], keypath_prefix + (k,))
        return paths
    elif isinstance(obj, list):
        for obj in obj:
            paths += _jmespath_paths(obj, keypath_prefix + ("[]",))
        return paths
    else:
        return [keypath_prefix]


def jmespath_paths(doc):
    return [".".join(p).replace(".[", "[").replace("-", "_")
            for p in _jmespath_paths(doc)]


def randitem(lst):
    return lst[randint(0, len(lst) - 1)]


def test_clauses(boolean_jmespath_clauses, doc):
    for jp in boolean_jmespath_clauses:
        jmespath.search(jp, doc)
    random_join = " && ".join([f"({randitem(boolean_jmespath_clauses)})" for _ in range(3)])
    assert jmespath.search(random_join, doc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fqid")
    parser.add_argument("--max-string-length", type=int, default=100)
    args = parser.parse_args()

    doc = build_bundle_metadata_document(Replica.aws, f"bundles/{args.fqid}")

    boolean_jmespath_clauses = list()
    for jp in jmespath_paths(doc):
        try:
            res = jmespath.search(jp, doc)
        except JMESPathError:
            print("FAILED:", jp)
            continue
        else:
            if isinstance(res, str) or isinstance(res, int):
                boolean_jmespath_clauses.append(jp + f"==`{res}`")
            elif isinstance(res, list):
                for i, obj in enumerate(res):
                    if isinstance(obj, str) and len(obj) > args.max_string_length:
                        continue
                    elif isinstance(obj, bool):
                        jps = jp + f" | [{i}]"
                        if not obj:
                            jps = f"!({jps})"
                        boolean_jmespath_clauses.append(jps)
                    else:
                        boolean_jmespath_clauses.append(jp + f" | contains(@, `{obj}`)")
            else:
                raise Exception(f"no handler for object of type {type(res)}")

    test_clauses(boolean_jmespath_clauses, doc)
    for p in boolean_jmespath_clauses:
        print(p)
