# -*- coding: utf-8 -*-
"""My PDS Module."""
import pds.registrysweepers.provenance
import pkg_resources


__version__ = pkg_resources.resource_string(__name__, "VERSION.txt").decode("utf-8").strip()


# For future consideration:
#
# - Other metadata (__docformat__, __copyright__, etc.)
# - N̶a̶m̶e̶s̶p̶a̶c̶e̶ ̶p̶a̶c̶k̶a̶g̶e̶s̶ we got this
#
# Trivial change: Fri Mar 22 19:15:37 UTC 2024
