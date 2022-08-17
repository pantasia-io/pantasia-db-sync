from __future__ import annotations

from pydantic import BaseSettings


class Settings(BaseSettings):
    """Application settings with default values"""
    time_interval: int = 120
    # Pantasia DB
    environment: str = 'dev'
    db_host: str = 'localhost'
    db_port: int = 5432
    db_user: str = 'postgres'
    db_pass: str = 'postgres'
    db_name: str = 'pantasia'
    db_echo: bool = False

    # Cardano DB
    cdb_host: str = 'localhost'
    cdb_port: int = 5433
    cdb_user: str = 'postgres'
    cdb_pass: str = 'postgres'
    cdb_name: str = 'cexplorer'
    cdb_echo: bool = False

    class Config:
        env_file = '.env'
        env_prefix = 'PANTASIA_'
        env_file_encoding = 'utf-8'


settings = Settings()
