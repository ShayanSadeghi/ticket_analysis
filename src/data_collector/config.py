import os
import re
from typing import Any, Dict, List

import yaml
from pydantic import BaseModel, field_validator


class FieldConfig(BaseModel):
    name: str
    field_type: str


class TableConfig(BaseModel):
    table_name: str
    engine: str
    order_by: List[str]
    partition_by: str
    fields: List[FieldConfig]

    @classmethod
    def from_yaml(cls, path: str) -> "TableConfig":
        with open(path) as f:
            data = yaml.safe_load(f)["table_config"]
        return cls(**data)


class ClickHouseConfig(BaseModel):
    host: str
    port: int
    user: str
    password: str
    database: str

    @field_validator("password")
    @classmethod
    def password_not_empty(cls, v):
        if not v:
            raise ValueError("ClickHouse password cannot be empty")
        return v


class Config(BaseModel):
    clickhouse: ClickHouseConfig
    sources: Dict[str, Any]
    routes: List[Any]
    departure_windows: List[int]
    rate_limit_delay: float = 1.0

    @classmethod
    def from_yaml_with_env(cls, path: str) -> "Config":
        with open(path) as f:
            data = yaml.safe_load(f)

        # Pass cls to ensure inheritance works correctly
        data = cls._substitute_env_vars(data)
        return cls(**data)

    @classmethod
    def _substitute_env_vars(cls, data):
        if isinstance(data, dict):
            return {k: cls._substitute_env_vars(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [cls._substitute_env_vars(item) for item in data]
        elif isinstance(data, str):
            # Use regex to find ${VAR} patterns anywhere in the string
            def replace(match):
                env_var = match.group(1)
                return os.getenv(env_var, match.group(0))

            return re.sub(r"\$\{([^}]+)\}", replace, data)
        return data
