#!/usr/bin/env python
# This script is for getting more information out of the DSS ES instance about links.json files.
# Source your environment correctly `source environment && source environment.{stage}`
# Make sure to set your DSS_ES_ENDPOINT environment variable, this can be retrieved from running
# `dssops lambda environment`
# the ES instance has Access Control based on IP, so you'll have to change the access policy
# Make sure to change it back, or you're gonna break CD pipelines.
# Run the script with `python links_inspection.py`
# This should have been kept inside the Azul/Attic, but there are DSS specific classes
# See Azul/1727

from __future__ import print_function
from sys import getsizeof, stderr
from itertools import chain
from collections import deque
try:
    from reprlib import repr
except ImportError:
    pass

import deepdiff
import pprint
import copy
import os
import sys

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from dss.index.es import ElasticsearchClient
from dss import Config, Replica, ESIndexType, dss_handler, DSSException
from dss import ESDocType


# see last paragraph of https://docs.python.org/3/library/sys.html#sys.getsizeof
def total_size(o, handlers={}, verbose=False):
    """ Returns the approximate memory footprint an object and all of its contents.

    Automatically finds the contents of the following builtin containers and
    their subclasses:  tuple, list, deque, dict, set and frozenset.
    To search other containers, add handlers to iterate over their contents:

        handlers = {SomeContainerClass: iter,
                    OtherContainerClass: OtherContainerClass.get_elements}

    """
    dict_handler = lambda d: chain.from_iterable(d.items())
    all_handlers = {tuple: iter,
                    list: iter,
                    deque: iter,
                    dict: dict_handler,
                    set: iter,
                    frozenset: iter,
                   }
    all_handlers.update(handlers)     # user handlers take precedence
    seen = set()                      # track which object id's have already been seen
    default_size = getsizeof(0)       # estimate sizeof object without __sizeof__

    def sizeof(o):
        if id(o) in seen:       # do not double count the same object
            return 0
        seen.add(id(o))
        s = getsizeof(o, default_size)

        if verbose:
            print(s, type(o), repr(o), file=stderr)

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)


def _format_request_body(page: dict, es_query: dict, replica: Replica, output_format: str) -> dict:
    result_list = []  # type: typing.List[dict]
    for hit in page['hits']['hits']:
        result = {
            'bundle_fqid': hit['_id'],
            'search_score': hit['_score']
        }
        if output_format == 'raw':
            result['metadata'] = hit['_source']
        result_list.append(result)

    return {
        'es_query': es_query,
        'results': result_list,
        'total_hits': page['hits']['total']
    }


es_client = ElasticsearchClient.get()

replica = Replica.aws
es_query = { "query": { "bool": { "must": [ { "exists": { "field": "files.links_json"}} ] } } }
output_format = 'raw'
per_page = 1000
search_after = None

# Do not return the raw indexed data unless it is requested
if output_format != 'raw':
    es_query['_source'] = False

# https://www.elastic.co/guide/en/elasticsearch/reference/5.5/search-request-search-after.html
es_query['sort'] = [
    {"uuid": {"order": "desc"}},
    {"manifest.version": {"missing": "last", "order": "desc"}}
]


def search(search_after: str = None):
    if search_after is None:
        page = es_client.search(index=Config.get_es_alias_name(ESIndexType.docs, replica),
                                doc_type=ESDocType.doc.name,
                                size=per_page,
                                body=es_query,
                                )
    else:
        es_query['search_after'] = search_after.split(',')
        page = es_client.search(index=Config.get_es_alias_name(ESIndexType.docs, replica),
                                doc_type=ESDocType.doc.name,
                                size=per_page,
                                body=es_query,
                                )
    return page

total_hits = 0
current_page = 0
# changing this max_pages to 1-2 allows for testing, 300 is overkill it for this search we expect ~150 requests
max_pages = 300
processing_lookup = dict()
largest_link_json = None
largest_link_json_size = 0
largest_bundle = None

histogram = dict() # Key is the len of links in a links.json file, value is count.

def print_stats():
    print(f'total process hits: {total_hits}')
    print(f"total number of unique processes: {len(processing_lookup)}")
    # pprint.pprint(largest_link_json)
    print(f"size in bytes of largest links_json: {largest_link_json_size}")
    # print(largest_bundle)
    for k,v in histogram.items():
        histogram[k]['unique_process'] = len(v['unique_process']) # if you can remove this loop to inspect the sets.
    pprint.pprint(histogram)

    #pprint.pprint(processing_lookup)

while True:
    page = search(search_after)
    try:
        next_page = page['hits']['hits'][-1]['sort']
    except IndexError:
        print('i think we got everything')
        print_stats()
        break
    search_after = ','.join(page['hits']['hits'][-1]['sort'])
    current_page += 1
    fmt_page = _format_request_body(page,es_query,replica,output_format)
    for bundles in fmt_page['results']:

        # sizing stuff
        size = total_size(bundles['metadata']['files']['links_json'])
        if largest_link_json_size < size:
            largest_link_json_size = size
            largest_link_json = bundles['metadata']['files']['links_json']
            largest_bundle_fqid = bundles

        #histogram stuff
        number_of_links = len(bundles['metadata']['files']['links_json'][0]['links'])
        if histogram.get(number_of_links) is None:
            histogram[number_of_links] = {'number_of_links_json': 1, 'unique_process': set()}
        else:
            histogram[number_of_links]['number_of_links_json'] += 1

        # comparing links_json obj
        for link in bundles['metadata']['files']['links_json'][0]['links']:

            total_hits += 1
            processes = link['process']
            # add the processessID here to the unique_set
            histogram[number_of_links]['unique_process'].add(processes)

            if processes not in processing_lookup:
                processing_lookup[processes] = copy.deepcopy(link)
            else:
                difference = deepdiff.DeepDiff(link, processing_lookup[processes])
                if len(difference.keys()) == 0:
                    continue
                else:
                    print(f'WARNING:: process metadata DOES NOT match for collision: {processing_lookup} {link}')
    if max_pages <= current_page:
        print_stats()
        exit()
