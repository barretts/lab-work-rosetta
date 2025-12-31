"""Tests for the LabTestResolver."""

import pytest

from rosetta.core.resolver import LabTestResolver


@pytest.fixture
def resolver():
    """Create resolver instance."""
    r = LabTestResolver()
    yield r
    r.close()


class TestNormalize:
    """Tests for text normalization."""

    def test_lowercase(self, resolver):
        """Normalizes to lowercase."""
        assert resolver.normalize("HEMOGLOBIN") == "hemoglobin"

    def test_expands_abbreviations(self, resolver):
        """Expands known abbreviations."""
        assert "hemoglobin" in resolver.normalize("hgb")
        assert "alanine aminotransferase" in resolver.normalize("alt")

    def test_removes_suffixes(self, resolver):
        """Removes common suffixes."""
        assert resolver.normalize("sodium level") == "sodium"
        assert resolver.normalize("WBC count") == "white blood cell"


class TestResolve:
    """Tests for resolve method."""

    def test_returns_none_for_empty(self, resolver):
        """Returns None for empty input."""
        assert resolver.resolve("") is None
        assert resolver.resolve("  ") is None
        assert resolver.resolve("a") is None

    def test_returns_dict_for_match(self, resolver):
        """Returns dict with expected keys for match."""
        result = resolver.resolve("hemoglobin")
        if result:
            assert "loinc_code" in result
            assert "loinc_name" in result
            assert "confidence" in result
            assert "method" in result


class TestGetLoincDetails:
    """Tests for get_loinc_details method."""

    def test_returns_none_for_invalid(self, resolver):
        """Returns None for invalid LOINC code."""
        assert resolver.get_loinc_details("INVALID-999") is None

    def test_returns_dict_for_valid(self, resolver):
        """Returns dict for valid LOINC code."""
        # 718-7 is Hemoglobin
        result = resolver.get_loinc_details("718-7")
        if result:
            assert result["loinc_code"] == "718-7"
            assert "long_common_name" in result


class TestSearch:
    """Tests for search method."""

    def test_returns_list(self, resolver):
        """Search returns a list."""
        results = resolver.search("glucose")
        assert isinstance(results, list)

    def test_respects_limit(self, resolver):
        """Search respects limit parameter."""
        results = resolver.search("blood", limit=5)
        assert len(results) <= 5


class TestGetStats:
    """Tests for get_stats method."""

    def test_returns_dict(self, resolver):
        """Stats returns a dictionary."""
        stats = resolver.get_stats()
        assert isinstance(stats, dict)
        assert "LOINC codes" in stats
