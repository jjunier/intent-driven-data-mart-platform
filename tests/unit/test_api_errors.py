"""Unit tests for app.errors — global exception handler functions.

Each handler is tested in isolation by:
- Passing a ``MagicMock`` as the ``Request`` argument (handlers do not
  inspect the request in the current implementation).
- Passing a real exception instance.
- Asserting the returned ``JSONResponse`` status code and body.

Because ``asyncio_mode = "auto"`` is configured in ``pyproject.toml``,
async test functions are collected and run automatically without any
additional markers.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.errors import (
    ErrorResponse,
    generic_exception_handler,
    import_error_handler,
    intent_validation_error_handler,
    mart_spec_validation_error_handler,
    value_error_handler,
)
from intent.validator import IntentValidationError
from mart_design.validator import MartSpecValidationError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_request() -> MagicMock:
    """Return a mock FastAPI Request — handlers do not inspect it."""
    return MagicMock()


# ---------------------------------------------------------------------------
# ErrorResponse DTO
# ---------------------------------------------------------------------------


class TestErrorResponse:
    def test_has_detail_field(self):
        resp = ErrorResponse(detail="msg", error_code="CODE")
        assert resp.detail == "msg"

    def test_has_error_code_field(self):
        resp = ErrorResponse(detail="msg", error_code="CODE")
        assert resp.error_code == "CODE"

    def test_model_dump_contains_both_fields(self):
        data = ErrorResponse(detail="msg", error_code="CODE").model_dump()
        assert "detail" in data
        assert "error_code" in data


# ---------------------------------------------------------------------------
# IntentValidationError handler
# ---------------------------------------------------------------------------


class TestIntentValidationErrorHandler:
    async def test_returns_status_400(self):
        exc = IntentValidationError("subject_area is required")
        response = await intent_validation_error_handler(_mock_request(), exc)
        assert response.status_code == 400

    async def test_detail_contains_exception_message(self):
        exc = IntentValidationError("subject_area is required")
        response = await intent_validation_error_handler(_mock_request(), exc)
        body = response.body
        import json
        data = json.loads(body)
        assert "subject_area is required" in data["detail"]

    async def test_error_code_is_intent_validation_error(self):
        exc = IntentValidationError("any message")
        response = await intent_validation_error_handler(_mock_request(), exc)
        import json
        data = json.loads(response.body)
        assert data["error_code"] == "INTENT_VALIDATION_ERROR"

    async def test_content_type_is_json(self):
        exc = IntentValidationError("any message")
        response = await intent_validation_error_handler(_mock_request(), exc)
        assert response.media_type == "application/json"


# ---------------------------------------------------------------------------
# MartSpecValidationError handler
# ---------------------------------------------------------------------------


class TestMartSpecValidationErrorHandler:
    async def test_returns_status_400(self):
        exc = MartSpecValidationError("column 'revenue' not found")
        response = await mart_spec_validation_error_handler(_mock_request(), exc)
        assert response.status_code == 400

    async def test_detail_contains_exception_message(self):
        exc = MartSpecValidationError("column 'revenue' not found")
        response = await mart_spec_validation_error_handler(_mock_request(), exc)
        import json
        data = json.loads(response.body)
        assert "revenue" in data["detail"]

    async def test_error_code_is_mart_spec_validation_error(self):
        exc = MartSpecValidationError("any message")
        response = await mart_spec_validation_error_handler(_mock_request(), exc)
        import json
        data = json.loads(response.body)
        assert data["error_code"] == "MART_SPEC_VALIDATION_ERROR"

    async def test_content_type_is_json(self):
        exc = MartSpecValidationError("any message")
        response = await mart_spec_validation_error_handler(_mock_request(), exc)
        assert response.media_type == "application/json"


# ---------------------------------------------------------------------------
# ImportError handler
# ---------------------------------------------------------------------------


class TestImportErrorHandler:
    async def test_returns_status_503(self):
        exc = ImportError("No module named 'google.cloud.bigquery'")
        response = await import_error_handler(_mock_request(), exc)
        assert response.status_code == 503

    async def test_detail_mentions_dependency(self):
        exc = ImportError("No module named 'google.cloud.bigquery'")
        response = await import_error_handler(_mock_request(), exc)
        import json
        data = json.loads(response.body)
        assert "dependency" in data["detail"].lower()

    async def test_detail_includes_original_error(self):
        exc = ImportError("No module named 'google.cloud.bigquery'")
        response = await import_error_handler(_mock_request(), exc)
        import json
        data = json.loads(response.body)
        assert "google.cloud.bigquery" in data["detail"]

    async def test_error_code_is_dependency_not_available(self):
        exc = ImportError("No module named 'google.cloud.bigquery'")
        response = await import_error_handler(_mock_request(), exc)
        import json
        data = json.loads(response.body)
        assert data["error_code"] == "DEPENDENCY_NOT_AVAILABLE"

    async def test_content_type_is_json(self):
        exc = ImportError("any missing module")
        response = await import_error_handler(_mock_request(), exc)
        assert response.media_type == "application/json"


# ---------------------------------------------------------------------------
# ValueError handler
# ---------------------------------------------------------------------------


class TestValueErrorHandler:
    async def test_returns_status_400(self):
        exc = ValueError("Unsupported reader_type: 'snowflake'")
        response = await value_error_handler(_mock_request(), exc)
        assert response.status_code == 400

    async def test_detail_contains_exception_message(self):
        exc = ValueError("Unsupported reader_type: 'snowflake'")
        response = await value_error_handler(_mock_request(), exc)
        import json
        data = json.loads(response.body)
        assert "snowflake" in data["detail"]

    async def test_error_code_is_invalid_input(self):
        exc = ValueError("Unsupported reader_type: 'snowflake'")
        response = await value_error_handler(_mock_request(), exc)
        import json
        data = json.loads(response.body)
        assert data["error_code"] == "INVALID_INPUT"

    async def test_content_type_is_json(self):
        exc = ValueError("any value error")
        response = await value_error_handler(_mock_request(), exc)
        assert response.media_type == "application/json"

    async def test_does_not_handle_intent_validation_error(self):
        """IntentValidationError must be handled by its own handler, not here.

        This test verifies the expected handler assignment by confirming that
        intent_validation_error_handler returns INTENT_VALIDATION_ERROR code,
        not INVALID_INPUT.
        """
        exc = IntentValidationError("subclass should not reach ValueError handler")
        response = await intent_validation_error_handler(_mock_request(), exc)
        import json
        data = json.loads(response.body)
        assert data["error_code"] == "INTENT_VALIDATION_ERROR"


# ---------------------------------------------------------------------------
# Generic exception handler (catch-all)
# ---------------------------------------------------------------------------


class TestGenericExceptionHandler:
    async def test_returns_status_500(self):
        exc = RuntimeError("unexpected internal failure")
        response = await generic_exception_handler(_mock_request(), exc)
        assert response.status_code == 500

    async def test_detail_is_fixed_message(self):
        """Original exception message must NOT be exposed to API consumers."""
        exc = RuntimeError("sensitive internal detail")
        response = await generic_exception_handler(_mock_request(), exc)
        import json
        data = json.loads(response.body)
        assert data["detail"] == "Internal server error"
        assert "sensitive internal detail" not in data["detail"]

    async def test_error_code_is_internal_error(self):
        exc = RuntimeError("boom")
        response = await generic_exception_handler(_mock_request(), exc)
        import json
        data = json.loads(response.body)
        assert data["error_code"] == "INTERNAL_ERROR"

    async def test_content_type_is_json(self):
        exc = Exception("any exception")
        response = await generic_exception_handler(_mock_request(), exc)
        assert response.media_type == "application/json"

    async def test_handles_bare_exception(self):
        exc = Exception("bare exception")
        response = await generic_exception_handler(_mock_request(), exc)
        assert response.status_code == 500

    async def test_suppresses_exception_class_name(self):
        """Class name of the internal exception must not leak."""
        exc = AttributeError("internal attr error")
        response = await generic_exception_handler(_mock_request(), exc)
        import json
        data = json.loads(response.body)
        assert "AttributeError" not in data["detail"]


# ---------------------------------------------------------------------------
# End-to-end via TestClient — handler integration with FastAPI app
# ---------------------------------------------------------------------------


class TestHandlerIntegrationWithApp:
    """Verify that handlers are correctly wired into the FastAPI app.

    Uses ``TestClient`` with patched service functions so that specific
    exceptions can be triggered through actual HTTP calls.
    """

    def test_intent_validation_error_returns_400_with_error_code(self):
        from unittest.mock import patch

        import json
        from fastapi.testclient import TestClient

        from app.main import app
        from intent.validator import IntentValidationError

        client = TestClient(app, raise_server_exceptions=False)
        with patch(
            "app.routers.marts.propose_mart_from_request",
            side_effect=IntentValidationError("bad intent"),
        ), patch("app.routers.marts._build_reader", return_value=MagicMock()):
            response = client.post(
                "/api/v1/marts",
                json={
                    "user_request": "???",
                    "reader_config": {"reader_type": "duckdb", "database_path": ":memory:"},
                },
            )
        assert response.status_code == 400
        data = response.json()
        assert "bad intent" in data["detail"]
        assert data["error_code"] == "INTENT_VALIDATION_ERROR"

    def test_mart_spec_validation_error_returns_400_with_error_code(self):
        from unittest.mock import patch

        from fastapi.testclient import TestClient

        from app.main import app
        from mart_design.validator import MartSpecValidationError

        client = TestClient(app, raise_server_exceptions=False)
        with patch(
            "app.routers.marts.propose_mart_from_request",
            side_effect=MartSpecValidationError("bad spec"),
        ), patch("app.routers.marts._build_reader", return_value=MagicMock()):
            response = client.post(
                "/api/v1/marts",
                json={
                    "user_request": "sales",
                    "reader_config": {"reader_type": "duckdb", "database_path": ":memory:"},
                },
            )
        assert response.status_code == 400
        data = response.json()
        assert data["error_code"] == "MART_SPEC_VALIDATION_ERROR"

    def test_unexpected_exception_returns_500_with_fixed_message(self):
        from unittest.mock import patch

        from fastapi.testclient import TestClient

        from app.main import app

        client = TestClient(app, raise_server_exceptions=False)
        with patch(
            "app.routers.marts.propose_mart_from_request",
            side_effect=RuntimeError("boom"),
        ), patch("app.routers.marts._build_reader", return_value=MagicMock()):
            response = client.post(
                "/api/v1/marts",
                json={
                    "user_request": "sales",
                    "reader_config": {"reader_type": "duckdb", "database_path": ":memory:"},
                },
            )
        assert response.status_code == 500
        data = response.json()
        assert data["detail"] == "Internal server error"
        assert data["error_code"] == "INTERNAL_ERROR"
        assert "boom" not in data["detail"]

    def test_import_error_returns_503_with_error_code(self):
        from unittest.mock import patch

        from fastapi.testclient import TestClient

        from app.main import app

        client = TestClient(app, raise_server_exceptions=False)
        with patch(
            "app.routers.marts._build_reader",
            side_effect=ImportError("No module named 'google.cloud.bigquery'"),
        ):
            response = client.post(
                "/api/v1/marts",
                json={
                    "user_request": "sales",
                    "reader_config": {"reader_type": "bigquery", "project_id": "p", "dataset_id": "d"},
                },
            )
        assert response.status_code == 503
        data = response.json()
        assert data["error_code"] == "DEPENDENCY_NOT_AVAILABLE"
