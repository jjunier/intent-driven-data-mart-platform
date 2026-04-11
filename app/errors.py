"""Centralised exception handlers for the FastAPI application.

All domain-level exceptions raised in the service and router layers are
mapped to structured JSON responses here.  Handlers are registered in
``app.main`` via ``app.add_exception_handler``.

Error response shape
--------------------
All error responses share the ``ErrorResponse`` DTO::

    {
      "detail": "<human-readable message>",
      "error_code": "<machine-readable code>"
    }

The ``detail`` field is intentionally named to match FastAPI's default
error response key, preserving backward compatibility with existing
clients and tests.

Note: ``RequestValidationError`` (HTTP 422) is *not* overridden here;
FastAPI's default behaviour is preserved for this stage.
"""

from __future__ import annotations

from fastapi import Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from intent.validator import IntentValidationError
from mart_design.validator import MartSpecValidationError


class ErrorResponse(BaseModel):
    """Structured error response body returned for all handled exceptions."""

    detail: str
    error_code: str


# ---------------------------------------------------------------------------
# Handler functions
# ---------------------------------------------------------------------------


async def intent_validation_error_handler(
    request: Request,
    exc: IntentValidationError,
) -> JSONResponse:
    """Map ``IntentValidationError`` to HTTP 400.

    Raised when the user's natural language request cannot be parsed into a
    valid intent (e.g. missing metrics or unrecognised time granularity).
    """
    return JSONResponse(
        status_code=400,
        content=ErrorResponse(
            detail=str(exc),
            error_code="INTENT_VALIDATION_ERROR",
        ).model_dump(),
    )


async def mart_spec_validation_error_handler(
    request: Request,
    exc: MartSpecValidationError,
) -> JSONResponse:
    """Map ``MartSpecValidationError`` to HTTP 400.

    Raised when the LLM-generated mart specification references columns or
    tables that do not exist in the source schema.
    """
    return JSONResponse(
        status_code=400,
        content=ErrorResponse(
            detail=str(exc),
            error_code="MART_SPEC_VALIDATION_ERROR",
        ).model_dump(),
    )


async def import_error_handler(
    request: Request,
    exc: ImportError,
) -> JSONResponse:
    """Map ``ImportError`` to HTTP 503.

    Raised when an optional dependency (e.g. ``google-cloud-bigquery``) is
    not installed and the caller requests a reader that requires it.
    """
    return JSONResponse(
        status_code=503,
        content=ErrorResponse(
            detail=f"A required dependency is not installed. Details: {exc}",
            error_code="DEPENDENCY_NOT_AVAILABLE",
        ).model_dump(),
    )


async def value_error_handler(
    request: Request,
    exc: ValueError,
) -> JSONResponse:
    """Map ``ValueError`` to HTTP 400.

    Intended primarily for the ``_build_reader`` defensive branch that
    raises ``ValueError`` for an unsupported ``reader_type``.

    Note: ``IntentValidationError`` and ``MartSpecValidationError`` both
    inherit from ``ValueError`` but have their own handlers registered with
    higher MRO priority, so they will never reach this handler.
    """
    return JSONResponse(
        status_code=400,
        content=ErrorResponse(
            detail=str(exc),
            error_code="INVALID_INPUT",
        ).model_dump(),
    )


async def generic_exception_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    """Map all unhandled exceptions to HTTP 500.

    The original exception message is intentionally suppressed to avoid
    leaking internal implementation details to API consumers.
    """
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            detail="Internal server error",
            error_code="INTERNAL_ERROR",
        ).model_dump(),
    )
