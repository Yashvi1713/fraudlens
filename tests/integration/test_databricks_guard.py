import os

from src.common.databricks_client import get_access_token, is_databricks_configured


def test_databricks_guard_is_false_without_credentials(monkeypatch):
    monkeypatch.delenv("DATABRICKS_HOST", raising=False)
    monkeypatch.delenv("DATABRICKS_HTTP_PATH", raising=False)
    monkeypatch.delenv("DATABRICKS_CLUSTER_ID", raising=False)
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
    monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
    assert is_databricks_configured() is False


def test_databricks_guard_requires_token_even_when_host_and_http_path_are_present(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/example")
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
    monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
    assert is_databricks_configured() is False


def test_databricks_guard_accepts_oauth_client_credentials(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/example")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "client-id")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "client-secret")
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    assert is_databricks_configured() is True


def test_get_access_token_exchanges_oauth_client_credentials(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://example.cloud.databricks.com")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "client-id")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "client-secret")
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"access_token": "oauth-token"}

        text = "ok"

    def fake_post(url, data=None, auth=None, timeout=None):
        assert url == "https://example.cloud.databricks.com/oidc/v1/token"
        assert data == {"grant_type": "client_credentials", "scope": "all-apis"}
        assert auth == ("client-id", "client-secret")
        assert timeout == 30
        return FakeResponse()

    monkeypatch.setattr("requests.post", fake_post)
    assert get_access_token() == "oauth-token"
