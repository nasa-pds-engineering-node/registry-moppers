from dataclasses import dataclass
from typing import Dict


@dataclass
class Update:
    """Class representing an ES/OpenSearch database update to a single document"""

    id: str
    content: Dict
