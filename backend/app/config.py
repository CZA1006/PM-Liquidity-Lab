from typing import List

from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # CLOB
    CLOB_HTTP_BASE: str = "https://clob.polymarket.com"
    CLOB_WS_BASE: str = "wss://ws-subscriptions-clob.polymarket.com/ws"
    CLOB_WS_CHANNEL: str = "market"  # "market" or "user" (we only use market)

    # Gamma
    GAMMA_HTTP_BASE: str = "https://gamma-api.polymarket.com"

    # Probe defaults
    PROBE_KEYWORD: str = "bitcoin"
    PROBE_MARKETS_LIMIT: int = 50
    PROBE_PICK_TOKENS: int = 3

    # Calibration
    CALIBRATION_INTERVAL_SEC: int = 30
    CALIBRATION_TOP_N: int = 50

    # Liquidity metrics defaults
    LIQ_NOTIONALS: List[float] = [100.0, 500.0, 1000.0]
    LIQ_DEPTH_BPS_LIST: List[int] = [10, 25, 50, 100]

    # -----------------------------
    # Phase2: Monitor + Metrics SSE
    # -----------------------------
    MONITOR_AUTOSTART: bool = True
    # Prefer explicit token ids; if empty, fall back to keyword + pick count
    MONITOR_TOKEN_IDS: str = ""  # comma-separated token ids
    MONITOR_KEYWORD: str = "ethereum"
    MONITOR_PICK_TOKENS: int = 3
    # REST snapshot poll interval (seconds) for drift correction
    MONITOR_POLL_INTERVAL_SEC: int = 10
    # Drift threshold: any price-level mismatch above this count triggers rebase
    MONITOR_MISMATCH_LEVELS: int = 50
    # SSE heartbeat interval
    MONITOR_SSE_HEARTBEAT_SEC: int = 15
    # In-memory publish queue size per subscriber
    MONITOR_SSE_QUEUE_MAX: int = 2000

    # Storage
    DATA_DIR: str = "./data"
    RUN_ID: str = "local"

settings = Settings()
