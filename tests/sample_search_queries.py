"""Sample queries for testing elastic search"""

smartseq2_paired_ends_vx_query = \
    {
        'query': {
            'bool': {
                'must': [{
                    'match': {
                        "files.donor_organism_json.medical_history.smoking_history": "yes"
                    }
                }, {
                    'match': {
                        "files.specimen_from_organism_json.genus_species.text": "Homo sapiens"
                    }
                }, {
                    'match': {
                        "files.specimen_from_organism_json.organ.text": "brain"
                    }
                }]
            }
        }
    }
