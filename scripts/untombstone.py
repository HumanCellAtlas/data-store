##  FOR REGEX MATCHING 
#TEMPLATE_FQID_DEAD = [A-Za-z0-9]{8}-[A-Za-z0-9]{4}-[A-Za-z0-9]{4}-[A-Za-z0-9]{4}-[A-Za-z0-9]{12}.[0-9]{4}-[0-9]{2}-[A-Za-z0-9]{9}.[A-Za-z0-9]{7}.dead
#
## --------------------------------------------------------------
## bundle
## --------------------------------------------------------------
#def untombstone_bundle():
#   
#    """
#    deletes dead bundles and brings back original bundle 
#    """
#
#    if tombstoned_or_not_bundle is False:
#        # do Nothing
#    else:
#       
#        # delete/deindex dead bundle
#        # "updates/restores" original bundle
#        # reindex original bundle
#        deindex_dead_bundle()
#        update_og_bundle()
#        es_client.reindex()
#
#def tombstoned_or_not_bundle():
#    """
#    Check if tombstoned or not 
#    """
#    try:
#        dss_client.get_bundle()
#        return True
#    except SwaggerAPIException as e:
#       if e.code = "404" or e.code = "400":
#          return False
#     else:
#          raise
#
#
#def deindex_dead_bundle(key):
#    """
#    Removes dead bundle and removes from es
#    """
#
#    if key.endswith(".dead") and key.startswith("bundle/"):
#        es_client = get_es_client()
#        fqid = key.split("/")[1]
#        es_client.delete_by_query(
#                body= {"query":{#delets based off of fqid}}
#        )
#        log.info(f"removed dead bundle {fqid} from es")
#
#
#def update_og_bundle(key):
#    """
#    Finds original bundle and updates it
#    """
#
#    fqid = key.split("/")[1]
#    if re.match(TEMPLATE_FQID_DEAD, fqid):
#        uuid, version, dead_tag = key.split(".")
#    else:
#        version = dss_client.create_version()
#        uuid , dead_tag = key.split(".")
#    es_client.update_by_query(
#            body= {"query":{#updates based on origin uuid}}
#    )
#    log("Untombstoned original bundle {uuid}.{version}")
#
#
## --------------------------------------------------------------
## tombstone 
## --------------------------------------------------------------
#
#def untombstone_collection():
#    if tombstoned_or_not_collection is False:
#        # do nothing
#    else:
#        deindex_dead_reindex_collection()
#
#
#def tombstoned_or_not_collection():
#    """
#    checks for exiting collection 
#    """
#
#    try:
#        dss_client.get_collection()
#        return True
#    except SwaggerAPIException as e:
#        if e.code = "404" or e.code = "400"
#            return False
#    else:
#        raise
#
#
#def deindex_dead_reindex_collection(key, handle, bucket):
#    """
#    Finds and deletes dead colletion and restores old collection
#    """
#
#    if key.endswith(".dead") and key.startswith("collection/"):
#        fqid = key.split("/")[1]
#        if re.match(TEMPLATE_FQID_DEAD, fqid):
#            uuid, version, dead_tag = fqid.split(".")
#        else:
#            version = dss_client.create_version()
#            uuid, dead_tag  = fqid.split(".")
#        dynamodb_client.delete_item(**dead_query)
#        log(f"revmoed collection {fqid} from dynamodb")
#        dynamodb_client.put_item(**og_query)
#        log(f"restored original collection {uuid} {version}")
#
