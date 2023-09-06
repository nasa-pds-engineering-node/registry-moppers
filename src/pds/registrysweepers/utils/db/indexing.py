from opensearchpy import OpenSearch


def ensure_index_mapping(client: OpenSearch, index_name: str, property_name: str, property_type: str):
    """Provides an easy-to-use wrapper for ensuring the presence of a given property name/type in a given index"""
    client.indices.put_mapping(index=index_name, body={"properties": {property_name: {"type": property_type}}})
