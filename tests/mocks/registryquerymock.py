import json
from typing import Dict
from typing import Iterable


class RegistryQueryMock:
    """
    Supplies a mock implementatino of pds.registrysweepers.utils.query_registry_db.

    The filepath provided in the constructor should be to a JSON file, mapping string-identifiers onto iterable
    collections of dicts, each representing an OpenSearch "hit".
    """

    def __init__(self, data_filepath: str):
        self.data_filepath = data_filepath
        with open(data_filepath) as query_file:
            self.data = json.load(query_file)

    def get_mocked_query(self, id: str) -> Iterable[Dict]:
        try:
            return self.data[id]
        except KeyError:
            raise ValueError(f"Could not find query with id {id} in filepath {self.data_filepath}")
