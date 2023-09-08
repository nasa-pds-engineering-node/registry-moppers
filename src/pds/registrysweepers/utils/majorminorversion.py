from __future__ import annotations

import functools


@functools.total_ordering
class MajorMinorVersion:
    major_version_minimum = 0
    minor_version_minimum = 0

    def __init__(self, major_version: int, minor_version: int):
        if major_version < self.major_version_minimum:
            raise ValueError(f"major_version must be 0 or higher (got {major_version})")

        if minor_version < self.minor_version_minimum:
            raise ValueError(f"minor_version must be 0 or higher (got {minor_version})")

        self.major_version = major_version
        self.minor_version = minor_version

    @classmethod
    def from_string(cls, version_str: str):
        major_version_chunk, minor_version_chunk = version_str.split(".")

        major_version = int(major_version_chunk)
        minor_version = int(minor_version_chunk)

        return cls(major_version, minor_version)

    def __str__(self):
        return f"{self.major_version}.{self.minor_version}"

    def __hash__(self):
        return hash(str(self))

    def __repr__(self):
        return f"{self.__class__.__name__}({str(self)})"

    def __eq__(self, other):
        return self.major_version == other.major_version and self.minor_version == other.minor_version

    def __lt__(self, other):
        if self.major_version != other.major_version:
            return self.major_version < other.major_version
        elif self.minor_version != other.minor_version:
            return self.minor_version < other.minor_version
        else:
            return False
