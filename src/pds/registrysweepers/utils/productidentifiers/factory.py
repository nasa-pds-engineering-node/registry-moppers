from abc import ABC
from abc import abstractmethod

from pds.registrysweepers.utils.productidentifiers.pdslid import PdsLid
from pds.registrysweepers.utils.productidentifiers.pdslidvid import PdsLidVid
from pds.registrysweepers.utils.productidentifiers.pdsproductidentifier import PdsProductIdentifier


class PdsProductIdentifierFactory(ABC):
    @staticmethod
    @abstractmethod
    def from_string(identifier: str):
        if len(identifier) == 0:
            raise ValueError("Cannot instantiate a PdsProductIdentifier from an empty string")

        try:
            return (
                PdsLidVid.from_string(identifier)
                if PdsProductIdentifier.LIDVID_SEPARATOR in identifier
                else PdsLid.from_string(identifier)
            )
        except ValueError as err:
            raise ValueError(f'Failed to parse PdsProductIdentifier from string "{identifier}"')
