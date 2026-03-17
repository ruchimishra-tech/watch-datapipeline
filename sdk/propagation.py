"""
pipeline_run_id propagation helpers (execution.md Feature 1.4).

Provides utilities to carry the pipeline_run_id across system boundaries:
  - Environment variables  (subprocess / child jobs)
  - HTTP headers           (REST API calls between pipeline stages)
  - Kafka message headers  (event-driven pipelines)

All helpers are pure functions — no side effects, no SDK state required.
"""
import os
from typing import Optional

# Canonical env-var / header key names
_ENV_KEY = "WATCH_PIPELINE_RUN_ID"
_ENV_PIPELINE_ID = "WATCH_PIPELINE_ID"
_ENV_PIPELINE_NAME = "WATCH_PIPELINE_NAME"
_ENV_ENVIRONMENT = "WATCH_DEPLOYMENT_ENVIRONMENT"

_HTTP_HEADER_RUN_ID = "x-watch-pipeline-run-id"
_HTTP_HEADER_PIPELINE_ID = "x-watch-pipeline-id"
_HTTP_HEADER_ENVIRONMENT = "x-watch-deployment-environment"

_KAFKA_HEADER_RUN_ID = "watch-pipeline-run-id"
_KAFKA_HEADER_PIPELINE_ID = "watch-pipeline-id"
_KAFKA_HEADER_ENVIRONMENT = "watch-deployment-environment"


class RunContext:
    """
    Lightweight carrier for pipeline run identity.
    Constructed by Pipeline and passed to propagation helpers.
    """
    def __init__(
        self,
        run_id: str,
        pipeline_id: str,
        pipeline_name: str,
        environment: str,
    ):
        self.run_id = run_id
        self.pipeline_id = pipeline_id
        self.pipeline_name = pipeline_name
        self.environment = environment

    # ── Serialise ─────────────────────────────────────────────────────────

    def to_env_vars(self) -> dict[str, str]:
        """
        Returns a dict of env vars suitable for injecting into a subprocess.

        Usage:
            import subprocess
            env = {**os.environ, **run.to_env_vars()}
            subprocess.run(["python", "child_job.py"], env=env)
        """
        return {
            _ENV_KEY: self.run_id,
            _ENV_PIPELINE_ID: self.pipeline_id,
            _ENV_PIPELINE_NAME: self.pipeline_name,
            _ENV_ENVIRONMENT: self.environment,
        }

    def to_http_headers(self) -> dict[str, str]:
        """
        Returns HTTP headers for propagating run identity across REST calls.

        Usage:
            import requests
            requests.get(url, headers=run.to_http_headers())
        """
        return {
            _HTTP_HEADER_RUN_ID: self.run_id,
            _HTTP_HEADER_PIPELINE_ID: self.pipeline_id,
            _HTTP_HEADER_ENVIRONMENT: self.environment,
        }

    def to_kafka_headers(self) -> list[tuple[str, bytes]]:
        """
        Returns Kafka message headers for propagating run identity.

        Usage:
            producer.produce(topic, value=msg, headers=run.to_kafka_headers())
        """
        return [
            (_KAFKA_HEADER_RUN_ID, self.run_id.encode()),
            (_KAFKA_HEADER_PIPELINE_ID, self.pipeline_id.encode()),
            (_KAFKA_HEADER_ENVIRONMENT, self.environment.encode()),
        ]

    # ── Deserialise ───────────────────────────────────────────────────────

    @classmethod
    def from_env(cls) -> Optional["RunContext"]:
        """
        Reconstructs a RunContext from environment variables.
        Returns None if the required env vars are not set (not a child process).

        Usage (in child job):
            ctx = RunContext.from_env()
            with Pipeline(run_context=ctx, ...) as run:
                ...
        """
        run_id = os.getenv(_ENV_KEY)
        if not run_id:
            return None
        return cls(
            run_id=run_id,
            pipeline_id=os.getenv(_ENV_PIPELINE_ID, "unknown"),
            pipeline_name=os.getenv(_ENV_PIPELINE_NAME, "unknown"),
            environment=os.getenv(_ENV_ENVIRONMENT, "dev"),
        )

    @classmethod
    def from_http_headers(cls, headers: dict) -> Optional["RunContext"]:
        """
        Reconstructs a RunContext from HTTP request headers (case-insensitive).
        Returns None if the required header is not present.
        """
        # Normalise keys to lowercase for case-insensitive lookup
        lower = {k.lower(): v for k, v in headers.items()}
        run_id = lower.get(_HTTP_HEADER_RUN_ID)
        if not run_id:
            return None
        return cls(
            run_id=run_id,
            pipeline_id=lower.get(_HTTP_HEADER_PIPELINE_ID, "unknown"),
            pipeline_name="unknown",
            environment=lower.get(_HTTP_HEADER_ENVIRONMENT, "dev"),
        )

    @classmethod
    def from_kafka_headers(cls, headers: list[tuple[str, bytes]]) -> Optional["RunContext"]:
        """
        Reconstructs a RunContext from Kafka message headers.
        Returns None if the required header is not present.
        """
        header_map = {k: v.decode(errors="replace") for k, v in (headers or [])}
        run_id = header_map.get(_KAFKA_HEADER_RUN_ID)
        if not run_id:
            return None
        return cls(
            run_id=run_id,
            pipeline_id=header_map.get(_KAFKA_HEADER_PIPELINE_ID, "unknown"),
            pipeline_name="unknown",
            environment=header_map.get(_KAFKA_HEADER_ENVIRONMENT, "dev"),
        )
