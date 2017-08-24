import datetime

smartseq2_paired_ends_query = \
    {
        'query': {
            'bool': {
                'must': [{
                    'match': {
                        "files.sample_json.donor.species": "Homo sapiens"
                    }
                }, {
                    'match': {
                        "files.assay_json.single_cell.method": "Fluidigm C1"
                    }
                }, {
                    'match': {
                        "files.sample_json.ncbi_biosample": "SAMN04303778"
                    }
                }]
            }
        }
    }


def get_version():
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H%M%S.%fZ")

