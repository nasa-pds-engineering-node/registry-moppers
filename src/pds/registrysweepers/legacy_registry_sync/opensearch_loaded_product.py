import elasticsearch

def get_cross_cluster_indices():
    # use the CCS connection aliases
    clusters = [
        "atm-prod-ccs",
        "geo-prod-ccs",
        "img-prod-ccs",
        "naif-prod-ccs",
        "ppi-prod-ccs",
        "psa-prod",
        "rms-prod",
        "sbnpsi-prod-ccs",
        "sbnumd-prod-ccs"
    ]
    indices = ["registry"]
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
