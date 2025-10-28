"""ws_de package - lightweight adapter layer for Fabric vs local.

This package provides a small adapter to abstract connection resolution between
Microsoft Fabric (notebookutils) and a local emulation (ConnectionManager).

Start here when refactoring code_utils into a stable package.
"""

from .adapter import Adapter

__all__ = ["Adapter"]
