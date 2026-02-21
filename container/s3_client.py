"""
S3 client for image storage (MinIO-compatible).

Provides both sync and async interfaces for upload, download, exists, delete.
"""

import asyncio
import logging
import os
import threading
from dataclasses import dataclass
from typing import List, Optional

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


@dataclass
class S3Config:
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    max_retries: int = 3
    connect_timeout: int = 10
    read_timeout: int = 30

    @classmethod
    def from_env(cls) -> "S3Config":
        return cls(
            endpoint=os.getenv("S3_ENDPOINT", "http://94.156.112.211:9000"),
            access_key=os.getenv("S3_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("S3_SECRET_KEY", "hPvCxU064y1nPAuHRPtHCow"),
            bucket=os.getenv("S3_BUCKET", "photos"),
        )


_default_config: Optional[S3Config] = None


def get_s3_config() -> S3Config:
    global _default_config
    if _default_config is None:
        _default_config = S3Config.from_env()
    return _default_config


def _make_boto_config(s3_config: S3Config) -> BotoConfig:
    return BotoConfig(
        retries={"max_attempts": s3_config.max_retries, "mode": "adaptive"},
        connect_timeout=s3_config.connect_timeout,
        read_timeout=s3_config.read_timeout,
    )


def _make_sync_client(s3_config: Optional[S3Config] = None):
    cfg = s3_config or get_s3_config()
    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
        config=_make_boto_config(cfg),
    )


# ---------------------------------------------------------------------------
# Sync interface
# ---------------------------------------------------------------------------

class S3Client:
    """Synchronous S3 client. Thread-safe for read operations."""

    def __init__(self, config: Optional[S3Config] = None):
        self._config = config or get_s3_config()
        self._client = _make_sync_client(self._config)
        self._bucket = self._config.bucket

    def upload(self, key: str, data: bytes, content_type: str = "image/jpeg") -> str:
        self._client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )
        return key

    def download(self, key: str) -> bytes:
        resp = self._client.get_object(Bucket=self._bucket, Key=key)
        return resp["Body"].read()

    def exists(self, key: str) -> bool:
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
            return True
        except ClientError:
            return False

    def delete(self, key: str) -> None:
        self._client.delete_object(Bucket=self._bucket, Key=key)

    def download_many(self, keys: List[str]) -> dict[str, bytes]:
        result = {}
        for key in keys:
            try:
                result[key] = self.download(key)
            except ClientError as e:
                logger.warning(f"Failed to download {key}: {e}")
        return result

    def ensure_bucket(self) -> None:
        try:
            self._client.head_bucket(Bucket=self._bucket)
        except ClientError:
            self._client.create_bucket(Bucket=self._bucket)
            logger.info(f"Created S3 bucket: {self._bucket}")


# ---------------------------------------------------------------------------
# Async interface
# ---------------------------------------------------------------------------

class S3AsyncClient:
    """
    Async S3 client wrapping boto3 sync calls via run_in_executor.

    For MinIO with moderate concurrency this is simpler and more reliable
    than aiobotocore session management, while still providing non-blocking I/O.
    """

    def __init__(self, config: Optional[S3Config] = None):
        self._sync = S3Client(config)

    async def upload(self, key: str, data: bytes, content_type: str = "image/jpeg") -> str:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._sync.upload, key, data, content_type)

    async def download(self, key: str) -> bytes:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._sync.download, key)

    async def exists(self, key: str) -> bool:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._sync.exists, key)

    async def delete(self, key: str) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._sync.delete, key)

    async def upload_many(self, items: list[tuple[str, bytes]]) -> list[str]:
        """Upload multiple items concurrently. Returns list of uploaded keys."""
        tasks = [self.upload(key, data) for key, data in items]
        return await asyncio.gather(*tasks)

    async def download_many(self, keys: List[str]) -> dict[str, bytes]:
        """Download multiple keys concurrently. Skips failures with warning."""
        results: dict[str, bytes] = {}

        async def _fetch(key: str):
            try:
                data = await self.download(key)
                results[key] = data
            except Exception as e:
                logger.warning(f"Failed to download {key}: {e}")

        await asyncio.gather(*[_fetch(k) for k in keys])
        return results

    def ensure_bucket(self) -> None:
        self._sync.ensure_bucket()


# ---------------------------------------------------------------------------
# Module-level singletons (lazy)
# ---------------------------------------------------------------------------

_sync_instance: Optional[S3Client] = None
_async_instance: Optional[S3AsyncClient] = None
_sync_lock = threading.Lock()


def get_s3_client(config: Optional[S3Config] = None) -> S3Client:
    global _sync_instance
    if _sync_instance is None:
        with _sync_lock:
            if _sync_instance is None:
                _sync_instance = S3Client(config)
    return _sync_instance


def get_s3_async_client(config: Optional[S3Config] = None) -> S3AsyncClient:
    global _async_instance
    if _async_instance is None:
        _async_instance = S3AsyncClient(config)
    return _async_instance


# ---------------------------------------------------------------------------
# S3 key helpers
# ---------------------------------------------------------------------------

def zamer_image_key(avito_item_id: str, image_order: int) -> str:
    return f"zamer/{avito_item_id}/{image_order}.jpg"
