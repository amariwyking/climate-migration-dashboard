from .page1 import Page1
from ..utils import Page

from typing import Dict, Type


PAGE_MAP: Dict[str, Type[Page]] = {
    "About": Page1,
}

__all__ = ["PAGE_MAP"]