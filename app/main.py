"""Application entry point."""

from fastapi import FastAPI

from app.errors import (
    generic_exception_handler,
    import_error_handler,
    intent_validation_error_handler,
    mart_spec_validation_error_handler,
    value_error_handler,
)
from app.routers import marts
from intent.validator import IntentValidationError
from mart_design.validator import MartSpecValidationError

app = FastAPI(
    title="Intent-Driven Data Mart Platform",
    description="Analyzes data warehouse schemas and proposes data marts based on user intent.",
    version="0.1.0",
)

# ---------------------------------------------------------------------------
# Global exception handlers
#
# Registration order does not affect dispatch priority — Starlette resolves
# the correct handler via Python's MRO (``type(exc).__mro__``), so the most
# specific subclass handler always wins over a parent-class handler.
#
# Priority chain for ValueError subclasses:
#   IntentValidationError  -> intent_validation_error_handler  (400)
#   MartSpecValidationError -> mart_spec_validation_error_handler (400)
#   ValueError             -> value_error_handler               (400)
#   Exception              -> generic_exception_handler         (500)
# ---------------------------------------------------------------------------

app.add_exception_handler(IntentValidationError, intent_validation_error_handler)
app.add_exception_handler(MartSpecValidationError, mart_spec_validation_error_handler)
app.add_exception_handler(ImportError, import_error_handler)
app.add_exception_handler(ValueError, value_error_handler)
app.add_exception_handler(Exception, generic_exception_handler)

app.include_router(marts.router)


@app.get("/health", tags=["health"])
async def health_check():
    return {"status": "ok"}
