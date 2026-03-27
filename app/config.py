"""Application configuration loaded from environment variables."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    anthropic_api_key: str
    dw_type: str = "duckdb"
    dw_project: str = ""
    dw_dataset: str = ""
    dw_credentials_path: str = ""
    app_env: str = "development"
    log_level: str = "INFO"

    class Config:
        env_file = ".env"


settings = Settings()
