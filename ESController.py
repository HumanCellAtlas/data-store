import deepdiff
import sys
import pathlib
import pprint
from dss.index.es import ElasticsearchClient
from dss import Config, Replica, ESIndexType, dss_handler, DSSException
from dss import ESDocType


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



data_dir = pathlib.Path.joinpath(pathlib.Path(__file__,), '../data')

# swagger_url = 'https://dss.data.humancellatlas.org/v1/swagger.json'

# dss_client = hca.dss.DSSClient(swagger_url=swagger_url)

#
# for result in dss_client.post_search.iterator(**es_args):
#     # need to be able to get to search_after information, need to figure out how to pull from the headers.
#     pprint.pprint(result)
#     bundle_data = dict()
#     bundle_data['bundle_fqid'] = result['bundle_fqid']
#     bundle_data['bundle_url'] = result['bundle_url']
#     bundle_data['links_json'] = result['metadata']['files']['links_json']

# stash metadata into files.


es_client = ElasticsearchClient.get()

replica = Replica.aws
es_query = { "query": { "bool": { "must": [ { "exists": { "field": "files.links_json"}} ] } } }
output_format = 'raw'
per_page = 1000
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

search_after = None
total_hits = 0
current_page = 0
max_pages = 50
processing_lookup = dict()
largest_link_json = None
largest_link_json_size = 0

while True:
    page = search(search_after)
    if page['hits']['hits'][-1]['sort'] is None:
        print('i think we got everything')
        break
    else:
        search_after = ','.join(page['hits']['hits'][-1]['sort'])
        current_page += 1
    fmt_page = _format_request_body(page,es_query,replica,output_format)
    for bundles in fmt_page['results']:
        # sizing stuff
        size = sys.getsizeof(bundles['metadata']['files']['links_json'])
        if largest_link_json_size < size:
            largest_link_json_size = size
            largest_link_json = bundles['metadata']['files']['links_json']
        # comparing links_json obj
        for link in bundles['metadata']['files']['links_json'][0]['links']:
            total_hits += 1
            processes_id = link['process']
            if processes_id not in processing_lookup:
                processing_lookup[processes_id] = link
            else:
                difference = deepdiff.DeepDiff(link, processing_lookup[processes_id])
                if len(difference.keys()) == 0:
                    continue
                else:
                    print(f'WARNING:: process metadata DOES NOT match for collision: {processing_lookup} {link}')
    if max_pages <= current_page:
        print(f'total_hits: {total_hits}')
        print(len(processing_lookup))
        pprint.pprint(largest_link_json)
        print(largest_link_json_size)
        #pprint.pprint(processing_lookup)
        break