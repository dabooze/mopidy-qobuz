from datetime import datetime
from datetime import timedelta
import hashlib
import json
import logging
import os
import queue
import shutil
import threading
from typing import Any, Dict, Optional

import requests

logger = logging.getLogger(__name__)


class FileCacheStorage:
    def __init__(
        self,
        cache_dir: str = ".cache/tracks",
        max_age_days: int = 30,
        max_total_size_bytes: int = 1024 * 1024 * 1024,  # 1 GB default
    ):
        self.cache_dir = cache_dir
        self.max_age_days = max_age_days
        self.max_total_size_bytes = max_total_size_bytes

        os.makedirs(cache_dir, exist_ok=True)
        logger.debug("Cached dir created: %s", cache_dir)

    def _get_cache_path(self, key: str) -> str:
        safe_key = hashlib.md5(key.encode()).hexdigest()
        return os.path.join(self.cache_dir, f"{safe_key}.cache")

    def _get_total_cache_size(self) -> int:
        total_size = 0
        for entry in os.scandir(self.cache_dir):
            if entry.is_file():
                total_size += entry.stat().st_size
        return total_size

    def _cleanup_oldest_files(self) -> None:
        """
        Remove oldest files when cache exceeds size limit.
        Prioritizes removing expired and then oldest files.
        """
        # Get all cache files with their metadata and stats
        cache_files = []
        for entry in os.scandir(self.cache_dir):
            if entry.is_file():
                try:
                    with open(entry.path, "r") as f:
                        metadata = json.load(f)

                    stored_at = datetime.fromisoformat(metadata.get("stored_at", ""))
                    is_expired = datetime.now() - stored_at > timedelta(
                        days=self.max_age_days
                    )

                    cache_files.append(
                        {
                            "path": entry.path,
                            "size": entry.stat().st_size,
                            "stored_at": stored_at,
                            "is_expired": is_expired,
                        }
                    )
                except Exception:
                    # If metadata is corrupted, consider the file for removal
                    cache_files.append(
                        {
                            "path": entry.path,
                            "size": entry.stat().st_size,
                            "stored_at": datetime.min,
                            "is_expired": True,
                        }
                    )

        # First, remove expired files
        expired_files = [f for f in cache_files if f["is_expired"]]
        expired_files.sort(key=lambda x: x["stored_at"])

        for file in expired_files:
            try:
                os.remove(file["path"])
            except Exception:
                pass

        # If still over limit, remove oldest non-expired files
        if self._get_total_cache_size() > self.max_total_size_bytes:
            non_expired_files = [f for f in cache_files if not f["is_expired"]]
            non_expired_files.sort(key=lambda x: x["stored_at"])

            for file in non_expired_files:
                try:
                    os.remove(file["path"])
                    if self._get_total_cache_size() <= self.max_total_size_bytes:
                        break
                except Exception:
                    pass

    def store(self, key: str, value: Dict[str, Any]) -> None:
        try:
            current_size = self._get_total_cache_size()
            if current_size + len(str(value).encode()) > self.max_total_size_bytes:
                self._cleanup_oldest_files()

            cache_path = self._get_cache_path(key)
            value["stored_at"] = datetime.now().isoformat()
            with open(cache_path, "w") as f:
                import json

                json.dump(value, f)

        except Exception as e:
            logger.error(f"Error storing cache for {key}: {e}")

    def retrieve(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a cache entry, checking for expiration.

        :param key: Unique identifier for the cache entry
        :return: Cache entry or None if not found or expired
        """
        try:
            cache_path = self._get_cache_path(key)
            if not os.path.exists(cache_path):
                return None

            with open(cache_path, "r") as f:
                entry = json.load(f)

            stored_at = datetime.fromisoformat(entry.get("stored_at", ""))
            if datetime.now() - stored_at > timedelta(days=self.max_age_days):
                os.remove(cache_path)
                return None

            return entry
        except Exception as e:
            logger.error(f"Error retrieving cache for {key}: {e}")
            return None

    def delete(self, key: str) -> None:
        """
        Delete a specific cache entry.

        :param key: Unique identifier for the cache entry
        """
        try:
            cache_path = self._get_cache_path(key)
            if os.path.exists(cache_path):
                os.remove(cache_path)
        except Exception as e:
            logger.error(f"Error deleting cache for {key}: {e}")

    def clear(self) -> None:
        try:
            shutil.rmtree(self.cache_dir)
            os.makedirs(self.cache_dir, exist_ok=True)
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")


class TrackUrlCache:
    """
    Comprehensive cache system for track URLs with flexible storage and download strategies.

    Allows configuring total cache size limit and other parameters.
    """

    def __init__(
        self,
        storage_adapter: FileCacheStorage = None,
        download_timeout: int = 30,
        max_retries: int = 3,
    ):
        """
        Initialize the track URL cache.

        :param storage_adapter: Cache storage strategy (defaults to FileCacheStorage)
        :param download_timeout: Timeout for downloading track URLs
        :param max_retries: Maximum number of download retry attempts
        :param max_cache_size_bytes: Maximum total cache size in bytes
        """
        self.storage = storage_adapter or FileCacheStorage(
            max_total_size_bytes=max_cache_size_bytes
        )
        self.download_timeout = download_timeout
        self.max_retries = max_retries

    def _cache_track_url(
        self,
        track_id: str,
        url: str,
        additional_metadata: Dict[str, Any] = None,
        timeout=0.1,
    ) -> Optional[str]:
        # Check if already cached
        cached_entry = self.storage.retrieve(track_id)
        if cached_entry and "local_path" in cached_entry:
            logger.info(f"Using cached track for {track_id}")
            return cached_entry["local_path"]

        # Create cache filename
        cache_filename = hashlib.md5(track_id.encode()).hexdigest() + ".track"
        cache_path = os.path.join(self.storage.cache_dir, cache_filename)

        # Ensure cache directory exists
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)

        # Download with retry logic
        for attempt in range(self.max_retries):
            try:
                response = requests.get(url, timeout=self.download_timeout)
                response.raise_for_status()

                # Save downloaded content
                with open(cache_path, "wb") as f:
                    f.write(response.content)

                # Prepare metadata
                metadata = {
                    "track_id": track_id,
                    # "url": url,
                    "local_path": cache_path,
                    "size_bytes": len(response.content),
                    **(additional_metadata or {}),
                }

                # Store cache entry
                self.storage.store(track_id, metadata)

                logger.info(f"Successfully cached track {track_id}")
                return cache_path

            except requests.RequestException as e:
                logger.warning(f"Download attempt {attempt + 1} failed: {e}")
                if attempt == self.max_retries - 1:
                    logger.error(f"Failed to download track {track_id}")
                    return None

    def cache_track_url(
        self,
        track_id,
        url: str,
        additional_metadata: Dict[str, Any] = None,
        timeout=0.1,
    ):
        # Queue to store the result
        result_queue = queue.Queue()

        def cache_retrieval():
            try:
                cached_track = self._cache_track_url(track_id, url, additional_metadata)
                result_queue.put(cached_track)
            except Exception:
                result_queue.put(None)

        # Start the retrieval thread
        retrieval_thread = threading.Thread(target=cache_retrieval)
        retrieval_thread.start()

        # Wait for the thread to complete or timeout
        retrieval_thread.join(timeout=timeout)

        try:
            # Try to get the result immediately
            cached_track = result_queue.get(block=False)

            if cached_track is not None:
                return cached_track

            threading.Thread(
                target=self._background_cache_retrieval,
                args=(track_id, url, additional_metadata),
            ).start()
            return None

        except queue.Empty:
            threading.Thread(
                target=self._background_cache_retrieval,
                args=(track_id, url, additional_metadata),
            ).start()
            return None

    def _background_cache_retrieval(self, track_id, url, additional_metadata):
        """
        Perform cache retrieval in the background.
        """
        try:
            cached_track = self._cache_track_url(track_id, url, additional_metadata)
            logger.debug(
                f"Background cache retrieval completed for track {cached_track}"
            )
        except Exception as e:
            logger.debug(f"Background cache retrieval failed for track {track_id}: {e}")

    def get_cached_track(self, track_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a cached track entry.

        :param track_id: Unique track identifier
        :return: Cached track metadata or None
        """
        return self.storage.retrieve(track_id)

    def delete_cached_track(self, track_id: str) -> None:
        """
        Remove a specific track from the cache.

        :param track_id: Unique track identifier
        """
        self.storage.delete(track_id)

    def clear_cache(self) -> None:
        """Clear entire track URL cache."""
        self.storage.clear()
