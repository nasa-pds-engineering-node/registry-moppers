import elasticsearch
import os

# Optional Environment variable  used for the Cross Cluster Search
# connections aliases. Each element is separated by a ","
CCS_CONN = "CCS_CONN"


def get_cross_cluster_indices():

    indices = ["registry"]

    if CCS_CONN in os.environ:
        clusters = os.environ[CCS_CONN].split(',')
        indices.extend([f"{c}:registry" for c in clusters])

    return indices


def get_already_loaded_lidvids(product_classes=[], es_conn=None):

    query = {
        "query": {
            "bool": {
                "should": [],
                "minimum_should_match": 1
            }
        },
        "fields": ["_id"]
    }

    prod_class_prop = "pds:Identification_Area/pds:product_class"
    query["query"]["bool"]["should"] = [
        dict(match_phrase={prod_class_prop: prod_class}) for prod_class in product_classes
    ]

    prod_id_resp = elasticsearch.helpers.scan(
        es_conn,
        index=get_cross_cluster_indices(),
        query=query,
        scroll="3m")
    return [p["_id"] for p in prod_id_resp]
