"""
Validation utilities for frictionless data package descriptors with
KBase credit metadata.

Validates two aspects of a descriptor:
1. Frictionless Data Package schema conformance (via the ``frictionless`` library).
2. KBase credit metadata schema conformance (via ``jsonschema`` against the
   published JSON Schema from https://github.com/kbase/credit_engine).
"""

import json
from pathlib import Path

import jsonschema
import requests
from frictionless import Package

# ---------------------------------------------------------------------------
# Credit metadata JSON Schema
# ---------------------------------------------------------------------------

CREDIT_SCHEMA_URL = (
    "https://raw.githubusercontent.com/kbase/credit_engine/"
    "develop/schema/dcm/jsonschema/credit_metadata.schema.json"
)

_credit_schema_cache: dict | None = None


def _fetch_credit_schema(url: str = CREDIT_SCHEMA_URL) -> dict:
    """Fetch the credit metadata JSON schema, caching it for the process lifetime."""
    global _credit_schema_cache
    if _credit_schema_cache is not None:
        return _credit_schema_cache
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    _credit_schema_cache = resp.json()
    return _credit_schema_cache


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


class ValidationResult:
    """Container for validation results."""

    def __init__(self) -> None:
        self.frictionless_errors: list[str] = []
        self.credit_errors: list[str] = []

    @property
    def is_valid(self) -> bool:
        return not self.frictionless_errors and not self.credit_errors

    def summary(self) -> str:
        """Return a human-readable summary string."""
        if self.is_valid:
            return "Descriptor is valid."
        parts: list[str] = []
        if self.frictionless_errors:
            parts.append(
                f"Frictionless errors ({len(self.frictionless_errors)}):\n  - "
                + "\n  - ".join(self.frictionless_errors)
            )
        if self.credit_errors:
            parts.append(
                f"Credit metadata errors ({len(self.credit_errors)}):\n  - "
                + "\n  - ".join(self.credit_errors)
            )
        return "\n".join(parts)


def validate_descriptor(
    descriptor: dict | str | Path,
    *,
    credit_schema_url: str = CREDIT_SCHEMA_URL,
) -> ValidationResult:
    """Validate a frictionless data package descriptor.

    Parameters
    ----------
    descriptor:
        Either a ``dict`` containing the descriptor, or a path (``str`` /
        ``Path``) to a JSON file.
    credit_schema_url:
        URL from which to fetch the KBase credit metadata JSON schema.
        Defaults to the canonical location on the ``develop`` branch.

    Returns
    -------
    ValidationResult
        Object with ``frictionless_errors``, ``credit_errors``, and helpers
        ``is_valid`` / ``summary()``.
    """
    if isinstance(descriptor, (str, Path)):
        descriptor = json.loads(Path(descriptor).read_text())

    result = ValidationResult()

    # --- 1. Frictionless validation ---
    for error in Package.metadata_validate(descriptor):
        result.frictionless_errors.append(error.message)

    # --- 2. Credit metadata validation ---
    credit = descriptor.get("credit")
    if credit is not None:
        schema = _fetch_credit_schema(credit_schema_url)
        validator = jsonschema.Draft201909Validator(schema)
        for error in sorted(validator.iter_errors(credit), key=lambda e: list(e.path)):
            result.credit_errors.append(
                f"{'.'.join(str(p) for p in error.absolute_path) or '<root>'}: "
                f"{error.message}"
            )

    return result
