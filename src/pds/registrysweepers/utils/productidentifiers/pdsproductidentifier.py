from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pds.registrysweepers.utils.productidentifiers.pdslid import PdsLid

from abc import ABC, abstractmethod


class PdsProductIdentifier(ABC):
    LIDVID_SEPARATOR = "::"

    @property
    @abstractmethod
    def lid(self) -> PdsLid:
        pass

    @abstractmethod
    def __str__(self):
        pass

    @staticmethod
    @abstractmethod
    def from_string(identifier: str):
        pass

    def is_bundle(self):
        """Return whether this identifier refers to a bundle product"""
        return self.lid.is_bundle()

    def is_collection(self):
        """Return whether this identifier refers to a collection product"""
        return self.lid.is_collection()

    def is_basic_product(self):
        """
        Return whether this identifier refers to a basic (i.e. non-aggregate) product, as distinct from a bundle or
        collection.
        """
        return self.lid.is_basic_product()

    @property
    def national_agency_name(self) -> str:
        return self.lid.national_agency_name

    @property
    def archiving_agency_name(self) -> str:
        return self.lid.archiving_agency_name

    @property
    def bundle_name(self) -> str:
        return self.lid.bundle_name

    # TODO: Consider moving the following properties to interface-like classes for multiple inheritance  if possible...
    #  probably it's more trouble than it's worth due to added complexity, but it would allow for the removal of the
    #  None types from the possible returns.

    @property
    def collection_name(self) -> str | None:
        return self.lid.collection_name

    @property
    def basic_product_name(self) -> str | None:
        return self.lid.basic_product_name

    @property
    def parent_bundle_lid(self) -> PdsLid | None:
        return self.lid.parent_bundle_lid

    @property
    def parent_collection_lid(self) -> PdsLid | None:
        return self.lid.parent_collection_lid
