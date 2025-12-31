"""Tests for the Flask API."""

import pytest

from rosetta.api import create_app


@pytest.fixture
def app():
    """Create application for testing."""
    app = create_app()
    app.config["TESTING"] = True
    return app


@pytest.fixture
def client(app):
    """Create test client."""
    return app.test_client()


class TestHealthEndpoint:
    """Tests for /health endpoint."""

    def test_health_returns_status(self, client):
        """Health endpoint returns status."""
        response = client.get("/health")
        assert response.status_code == 200
        data = response.get_json()
        assert "status" in data


class TestIndexEndpoint:
    """Tests for / endpoint."""

    def test_index_returns_api_info(self, client):
        """Index returns API information."""
        response = client.get("/")
        assert response.status_code == 200
        data = response.get_json()
        assert data["name"] == "Clinical Rosetta Stone API"
        assert "version" in data
        assert "endpoints" in data


class TestTranslateEndpoint:
    """Tests for /translate endpoint."""

    def test_translate_requires_query(self, client):
        """Translate requires q parameter."""
        response = client.get("/translate")
        assert response.status_code == 400
        data = response.get_json()
        assert "error" in data

    def test_translate_returns_result(self, client):
        """Translate returns result for valid query."""
        response = client.get("/translate?q=hemoglobin")
        assert response.status_code == 200
        data = response.get_json()
        assert "status" in data
        assert "input" in data


class TestSearchEndpoint:
    """Tests for /search endpoint."""

    def test_search_requires_query(self, client):
        """Search requires q parameter."""
        response = client.get("/search")
        assert response.status_code == 400

    def test_search_returns_results(self, client):
        """Search returns results."""
        response = client.get("/search?q=glucose")
        assert response.status_code == 200
        data = response.get_json()
        assert "results" in data


class TestBatchTranslate:
    """Tests for /translate/batch endpoint."""

    def test_batch_requires_json(self, client):
        """Batch translate requires JSON content type."""
        response = client.post("/translate/batch", data="test")
        assert response.status_code == 400

    def test_batch_requires_array(self, client):
        """Batch translate requires JSON array."""
        response = client.post(
            "/translate/batch",
            json={"test": "value"},
            content_type="application/json",
        )
        assert response.status_code == 400

    def test_batch_translates_array(self, client):
        """Batch translate processes array."""
        response = client.post(
            "/translate/batch",
            json=["hemoglobin", "glucose"],
            content_type="application/json",
        )
        assert response.status_code == 200
        data = response.get_json()
        assert "results" in data
        assert len(data["results"]) == 2
