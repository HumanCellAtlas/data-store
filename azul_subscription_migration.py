secondary_analysis_subscription_migration = dict()

secondary_analysis_subscription_migration['eb02b7c7-4afb-4499-8baa-3b4f4fdd114d'] = {
    'owner': "azul-indexer-integration@human-cell-atlas-travis-test.iam.gserviceaccount.com",
    'es_query': {
        'query': {
            'bool': {
                'must_not': [
                    {
                        'term': {
                            "admin_deleted": true
                        }
                    }
                ],
                'must': [
                    {
                        'exists': {
                            'field': "files.project_json"
                        }
                    },
                    {
                        'range': {
                            'manifest.version': {
                                'gte': "2018-11-27"
                            }
                        }
                    }
                ]
            }
        }
    },
    'jmespath_query' = (
        "event_type==`CREATE`"
        " && files.project_json != `null`"
    )
}

secondary_analysis_subscription_migration['4bd8ccea-c396-4a1c-bcad-017aea02a018'] = {
    'owner': "bluebox-subscription-manager@broad-dsde-mint-test.iam.gserviceaccount.com",
    'es_query': {
        'query': {
            'bool': {
                'must': [
                    {
                        'term': {
                            "admin_deleted": true
                        }
                    }
                ]
            }
        }
    },
    'jmespath_query':
        "event_type==`TOMBSTONE` || event_type=`DELETE`"
    )   
}
