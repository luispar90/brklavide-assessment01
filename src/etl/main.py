import logging
import os

from src.etl.api_client import ApiClient
from src.etl.db import build_engine, PostgresRepository
from src.etl.etl import ObrasETL


def load_env(name: str, default: str | None = None, required: bool = False) -> str | None:
    v = os.getenv(name, default)
    if required and not v:
        raise ValueError(f"Missing required env var: {name}")
    return v

def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    log = logging.getLogger("main")

    # Load from ENV
    pg_dsn = load_env("PG_DSN", required=True)
    api_base_url = load_env("API_BASE_URL", required=True)
    api_user = load_env("API_USER", required=True)
    api_password = load_env("API_PASSWORD", required=True)
    timeout_seconds = int(load_env("HTTP_TIMEOUT", "20") or "20")
    schema = load_env("PG_SCHEMA")

    engine = build_engine(pg_dsn)
    repo = PostgresRepository(engine, schema=schema)

    api = ApiClient(api_base_url, timeout_seconds=timeout_seconds)
    api.auth(api_user, api_password)

    # Run ETL
    log.info("Starting ETL")
    ObrasETL(api=api, repo=repo).run()
    log.info("ETL finished")


if __name__ == "__main__":
    main()