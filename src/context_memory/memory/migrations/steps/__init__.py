from __future__ import annotations

# Reserved for concrete migration step modules, e.g.:
# - v1_to_v2.py
# - v2_to_v3.py
#
# Keep imports explicit in migrations.__init__ when steps are added.
from . import v1_to_v2  # noqa: F401
