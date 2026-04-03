"""Application entry point."""

from fastapi import FastAPI

from app.routers import marts

app = FastAPI(
    title="Intent-Driven Data Mart Platform",
    description="Analyzes data warehouse schemas and proposes data marts based on user intent.",
    version="0.1.0",
)

app.include_router(marts.router)


@app.get("/health", tags=["health"])
async def health_check():
    return {"status": "ok"}
