"""Tests for descriptor_validator module."""

import json
import pytest
from unittest.mock import patch

from kbase_transfers.descriptor_validator import validate_descriptor, ValidationResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

MINIMAL_CREDIT = {
    "identifier": "TEST:12345",
    "resource_type": "dataset",
    "titles": [{"title": "Test Dataset"}],
    "contributors": [
        {
            "contributor_type": "Person",
            "given_name": "Jane",
            "family_name": "Doe",
        }
    ],
}

VALID_DESCRIPTOR = {
    "name": "test-package",
    "resources": [
        {
            "name": "readme",
            "path": "README.md",
            "mediatype": "text/markdown",
        }
    ],
    "credit": MINIMAL_CREDIT,
}


def _fake_credit_schema():
    """Return a minimal but functional credit metadata JSON schema."""
    return {
        "$schema": "https://json-schema.org/draft/2019-09/schema",
        "type": "object",
        "required": ["identifier", "resource_type", "titles", "contributors"],
        "properties": {
            "identifier": {"type": "string", "pattern": "^[a-zA-Z0-9.-_]+:\\S"},
            "resource_type": {"type": "string", "enum": ["dataset"]},
            "titles": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["title"],
                    "properties": {"title": {"type": "string"}},
                },
            },
            "contributors": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "contributor_type": {
                            "type": "string",
                            "enum": ["Person", "Organization"],
                        },
                        "name": {"type": ["string", "null"]},
                        "given_name": {"type": ["string", "null"]},
                        "family_name": {"type": ["string", "null"]},
                    },
                },
            },
        },
    }


@pytest.fixture(autouse=True)
def _mock_credit_schema():
    """Patch the credit schema fetch so tests don't hit the network."""
    with patch(
        "kbase_transfers.descriptor_validator._fetch_credit_schema",
        return_value=_fake_credit_schema(),
    ):
        yield


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestValidationResult:
    def test_empty_result_is_valid(self):
        r = ValidationResult()
        assert r.is_valid
        assert r.summary() == "Descriptor is valid."

    def test_frictionless_errors(self):
        r = ValidationResult()
        r.frictionless_errors.append("some error")
        assert not r.is_valid
        assert "Frictionless errors" in r.summary()

    def test_credit_errors(self):
        r = ValidationResult()
        r.credit_errors.append("bad field")
        assert not r.is_valid
        assert "Credit metadata errors" in r.summary()


class TestValidateDescriptor:
    def test_valid_descriptor(self):
        result = validate_descriptor(VALID_DESCRIPTOR)
        assert result.is_valid, result.summary()

    def test_missing_resources(self):
        descriptor = {"name": "empty"}
        result = validate_descriptor(descriptor)
        # frictionless requires at least one resource
        assert not result.is_valid
        assert result.frictionless_errors

    def test_no_credit_key_skips_credit_validation(self):
        descriptor = {
            "name": "no-credit-pkg",
            "resources": [{"name": "f", "path": "f.txt"}],
        }
        result = validate_descriptor(descriptor)
        assert result.credit_errors == []

    def test_invalid_credit_identifier(self):
        bad_credit = {**MINIMAL_CREDIT, "identifier": "no-colon"}
        descriptor = {**VALID_DESCRIPTOR, "credit": bad_credit}
        result = validate_descriptor(descriptor)
        assert result.credit_errors

    def test_missing_credit_titles(self):
        bad_credit = {k: v for k, v in MINIMAL_CREDIT.items() if k != "titles"}
        descriptor = {**VALID_DESCRIPTOR, "credit": bad_credit}
        result = validate_descriptor(descriptor)
        assert result.credit_errors

    def test_invalid_resource_type(self):
        bad_credit = {**MINIMAL_CREDIT, "resource_type": "software"}
        descriptor = {**VALID_DESCRIPTOR, "credit": bad_credit}
        result = validate_descriptor(descriptor)
        assert result.credit_errors

    def test_from_file(self, tmp_path):
        path = tmp_path / "descriptor.json"
        path.write_text(json.dumps(VALID_DESCRIPTOR))
        result = validate_descriptor(path)
        assert result.is_valid, result.summary()

    def test_from_string_path(self, tmp_path):
        path = tmp_path / "descriptor.json"
        path.write_text(json.dumps(VALID_DESCRIPTOR))
        result = validate_descriptor(str(path))
        assert result.is_valid, result.summary()
