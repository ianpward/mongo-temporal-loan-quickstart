"""
Shared MongoDB connection helper for activities.

Motor (async pymongo) creates a connection pool on first use.
We keep a module-level client so activities running in the same
worker process share connections rather than re-connecting on every call.
"""

import os

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

_client: AsyncIOMotorClient | None = None


def _get_client() -> AsyncIOMotorClient:
    global _client
    if _client is None:
        _client = AsyncIOMotorClient(os.environ["MONGODB_URI"])
    return _client


async def get_collection(name: str) -> AsyncIOMotorCollection:
    """Return a Motor collection from the configured database."""
    db_name = os.environ.get("MONGODB_DATABASE", "loan_processing")
    return _get_client()[db_name][name]
