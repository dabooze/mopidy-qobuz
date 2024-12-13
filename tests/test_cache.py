from datetime import datetime
from datetime import timedelta
import hashlib
import json
import os
import tempfile
from unittest.mock import Mock
from unittest.mock import patch

import pytest
import requests

from mopidy_qobuz.cache import FileCacheStorage
from mopidy_qobuz.cache import TrackUrlCache


@pytest.fixture
def temp_cache_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def file_cache_storage(temp_cache_dir):
    return FileCacheStorage(
        cache_dir=temp_cache_dir,
        max_age_days=1,
        max_total_size_bytes=1024 * 1024,  # 1 MB
    )


@pytest.fixture
def mock_requests_get():
    """Mock requests.get to simulate downloads."""
    with patch("requests.get") as mock_get:
        mock_response = Mock()
        mock_response.content = b"Test track content"
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response
        yield mock_get


def test_file_cache_storage_basic_operations(file_cache_storage, temp_cache_dir):
    test_key = "test_track_123"
    test_value = {"url": "http://example.com/track", "local_path": "/path/to/track"}

    # Store the item
    file_cache_storage.store(test_key, test_value)

    # Retrieve the item
    retrieved = file_cache_storage.retrieve(test_key)
    assert retrieved is not None
    assert retrieved["url"] == test_value["url"]
    assert "stored_at" in retrieved


def test_file_cache_storage_expiration(file_cache_storage, temp_cache_dir):
    test_key = "expired_track"
    test_value = {
        "url": "http://example.com/expired_track",
        "local_path": "/path/to/expired_track",
    }

    # Create a cache entry with an old timestamp
    cache_path = os.path.join(
        temp_cache_dir, hashlib.md5(test_key.encode()).hexdigest() + ".cache"
    )

    # Manually create an expired cache entry
    with open(cache_path, "w") as f:
        expired_value = test_value.copy()
        expired_value["stored_at"] = (datetime.now() - timedelta(days=2)).isoformat()
        json.dump(expired_value, f)

    # Try to retrieve
    retrieved = file_cache_storage.retrieve(test_key)
    assert retrieved is None
    assert not os.path.exists(cache_path)


def test_file_cache_storage_size_limit(temp_cache_dir):
    # Create a small cache with tiny size limit
    small_cache = FileCacheStorage(
        cache_dir=temp_cache_dir,
        max_age_days=1,
        max_total_size_bytes=100,  # Very small limit
    )

    # Store multiple entries to trigger cleanup
    for i in range(10):
        small_cache.store(
            f"track_{i}",
            {
                "url": f"http://example.com/track_{i}",
                "large_data": "x" * 50,  # Each entry is 50 bytes
            },
        )

    total_size = sum(
        os.path.getsize(os.path.join(temp_cache_dir, f))
        for f in os.listdir(temp_cache_dir)
    )
    assert total_size <= 100 * 2


def test_track_url_cache_download(temp_cache_dir, mock_requests_get):
    track_cache = TrackUrlCache(
        storage_adapter=FileCacheStorage(
            cache_dir=temp_cache_dir, max_total_size_bytes=1024 * 1024
        )
    )

    # Test download
    track_id = "test_track_download"
    test_url = "http://example.com/track"

    # First download
    first_cache_path = track_cache.cache_track_url(track_id, test_url)
    assert first_cache_path is not None
    assert os.path.exists(first_cache_path)

    # Verify cache entry
    cached_track = track_cache.get_cached_track(track_id)
    assert cached_track is not None
    assert cached_track["local_path"] == first_cache_path

    # Subsequent retrieval should return same path
    second_cache_path = track_cache.cache_track_url(track_id, test_url)
    assert second_cache_path == first_cache_path


def test_track_url_cache_download_failure(temp_cache_dir):
    # Create TrackUrlCache with temp directory
    track_cache = TrackUrlCache(
        storage_adapter=FileCacheStorage(
            cache_dir=temp_cache_dir, max_total_size_bytes=1024 * 1024
        ),
        max_retries=3,
        download_timeout=1,
    )

    # Mock requests to always fail
    with patch("requests.get") as mock_get:
        mock_get.side_effect = requests.exceptions.RequestException("Download failed")

        # Test download failure
        track_id = "failed_track"
        test_url = "http://example.com/failed_track"

        # Should return None after exhausting retries
        cache_path = track_cache.cache_track_url(track_id, test_url)
        assert cache_path is None


def test_track_url_cache_additional_metadata(temp_cache_dir, mock_requests_get):
    track_cache = TrackUrlCache(
        storage_adapter=FileCacheStorage(
            cache_dir=temp_cache_dir, max_total_size_bytes=1024 * 1024
        )
    )

    # Download with additional metadata
    track_id = "metadata_track"
    test_url = "http://example.com/metadata_track"
    additional_data = {"artist": "Test Artist", "album": "Test Album"}

    cache_path = track_cache.cache_track_url(
        track_id, test_url, additional_metadata=additional_data
    )

    # Verify cache entry includes additional metadata
    cached_track = track_cache.get_cached_track(track_id)
    assert cached_track is not None
    assert cached_track["artist"] == "Test Artist"
    assert cached_track["album"] == "Test Album"


def test_track_url_cache_clear(temp_cache_dir, mock_requests_get):
    track_cache = TrackUrlCache(
        storage_adapter=FileCacheStorage(
            cache_dir=temp_cache_dir, max_total_size_bytes=1024 * 1024
        )
    )

    # Download some tracks
    for i in range(5):
        track_cache.cache_track_url(f"track_{i}", f"http://example.com/track_{i}")

    # Verify tracks are cached
    assert len(os.listdir(temp_cache_dir)) > 0

    # Clear cache
    track_cache.clear_cache()

    # Verify cache is empty
    assert len(os.listdir(temp_cache_dir)) == 0


def test_track_url_cache_delete_specific_track(temp_cache_dir, mock_requests_get):
    track_cache = TrackUrlCache(
        storage_adapter=FileCacheStorage(
            cache_dir=temp_cache_dir, max_total_size_bytes=1024 * 1024
        )
    )

    # Download multiple tracks
    track_ids = ["track_1", "track_2", "track_3"]
    for track_id in track_ids:
        track_cache.cache_track_url(track_id, f"http://example.com/{track_id}")

    # Delete specific track
    track_cache.delete_cached_track("track_2")

    # Verify track is removed
    assert track_cache.get_cached_track("track_2") is None

    # Verify other tracks remain
    assert track_cache.get_cached_track("track_1") is not None
    assert track_cache.get_cached_track("track_3") is not None
