|Actions|	Resource|
|-------|-----------|
|dss:GetCheckout	|arn:hca:dss:{stage}:{replica}:checkout/{checkout_job_id}
|dss:DeleteBundle	|arn:hca:dss:{stage}:{replica}:bundles/{uuid}.{version}
|dss:GetBundle	|arn:hca:dss:{stage}:{replica}:bundles/{uuid}.{version}
|dss:PatchBundle	|arn:hca:dss:{stage}:{replica}:bundles/{uuid}.{version}
|dss:PutBundle	|arn:hca:dss:{stage}:{replica}:bundles/{uuid}.{version}
|dss:PostCheckout	|arn:hca:dss:{stage}:{replica}:bundles/{uuid}.{version}
|dss:GetCollections	|arn:hca:dss:{stage}:{replica}:{user}/collections
|dss:PutCollection	|arn:hca:dss:{stage}:{replica}:collections/{uuid}.{version}, arn:hca:dss:{stage}:{replica}:{user}/collections/{uuid}
|dss:DeleteCollection	|arn:hca:dss:{stage}:{replica}:collections/{uuid}.{version},  arn:hca:dss:{stage}:{replica}:{user}/collections/{uuid}
|dss:GetCollection	|arn:hca:dss:{stage}:{replica}:collections/{uuid}.{version},  arn:hca:dss:{stage}:{replica}:{user}/collections/{uuid}
|dss:PatchCollection	|arn:hca:dss:{stage}:{replica}:collections/{uuid}.{version},  arn:hca:dss:{stage}:{replica}:{user}/collections/{uuid}
|dss:GetFiles	|arn:hca:dss:{stage}:{replica}:files/{uuid}.{version}
|dss:HeadFiles	|arn:hca:dss:{stage}:{replica}:files/{uuid}.{version}
|dss:PutFiles	|arn:hca:dss:{stage}:{replica}:files/{uuid}.{version}
|dss:PostSearch	|arn:hca:dss:{stage}:{replica}:query
|dss:GetSubscriptions	|arn:hca:dss:{stage}:{replica}:{user}/subscriptions
|dss:PutSubscriptions	|arn:hca:dss:{stage}:{replica}:subscription/{uuid}, arn:hca:dss:{stage}:{replica}:{user}/subscription/{uuid}
|dss:GetSubscription	|arn:hca:dss:{stage}:{replica}:subscription/{uuid}, arn:hca:dss:{stage}:{replica}:{user}/subscription/{uuid}
|dss:DeleteSubscription	|arn:hca:dss:{stage}:{replica}:subscription/{uuid}, arn:hca:dss:{stage}:{replica}:{user}/subscription/{uuid}