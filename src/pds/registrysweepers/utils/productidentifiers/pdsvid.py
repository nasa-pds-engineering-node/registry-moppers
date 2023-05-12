from __future__ import annotations

import functools
import logging


@functools.total_ordering
class PdsVid:
    def __init__(self, major_version: int, minor_version: int):
        if major_version < 0:
            raise ValueError(f"major_version must be 0 or higher (got {major_version})")

        if minor_version < 0:
            raise ValueError(f"minor_version must be 0 or higher (got {minor_version})")

        self.major_version = major_version
        self.minor_version = minor_version

    @staticmethod
    def from_string(vid_string: str) -> PdsVid:
        major_version_chunk, minor_version_chunk = vid_string.split(".")

        major_version = int(major_version_chunk)
        minor_version = int(minor_version_chunk)

        return PdsVid(major_version, minor_version)

    def __str__(self):
        return f"{self.major_version}.{self.minor_version}"

    def __hash__(self):
        return hash(str(self))

    def __repr__(self):
        return f"PdsVid({str(self)})"

    def __eq__(self, other):
        return self.major_version == other.major_version and self.minor_version == other.minor_version

    def __lt__(self, other: PdsVid):
        if self.major_version != other.major_version:
            return self.major_version < other.major_version
        elif self.minor_version != other.minor_version:
            return self.minor_version < other.minor_version
        else:
            return False
