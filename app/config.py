from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class Settings:
    gemini_api_key: str = os.getenv("GEMINI_API_KEY", "").strip()
    gemini_model: str = os.getenv("GEMINI_MODEL", "gemini-1.5-flash").strip()
    allowed_root: str = os.getenv("EXCELMIND_ALLOWED_ROOT", "").strip()
    cors_origins_raw: str = os.getenv("EXCELMIND_CORS_ORIGINS", "*").strip()
    host: str = os.getenv("EXCELMIND_HOST", "0.0.0.0").strip() or "0.0.0.0"
    port: int = 8000
    reload: bool = os.getenv("EXCELMIND_RELOAD", "").strip().lower() in {"1", "true", "yes", "on"}

    def __post_init__(self) -> None:
        self.port = self._parse_port(os.getenv("EXCELMIND_PORT", "8000"))

    @property
    def allowed_root_path(self) -> Path | None:
        if not self.allowed_root:
            return None
        return Path(self.allowed_root).expanduser().resolve()

    @property
    def cors_origins(self) -> list[str]:
        origins = [origin.strip() for origin in self.cors_origins_raw.split(",")]
        return [origin for origin in origins if origin] or ["*"]

    @staticmethod
    def _parse_port(raw_value: str) -> int:
        try:
            return int(raw_value.strip() or "8000")
        except ValueError:
            return 8000


settings = Settings()
