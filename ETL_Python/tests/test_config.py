"""Tests for AppConfig module."""
import json
import os
import tempfile
from ETL_Python.config import AppConfig, PathsConfig, RcloneSettings


def test_default_config():
    """Test default configuration."""
    from ETL_Python.config import DuckDbSettings, NdjsonSettings, DownloaderSettings
    
    config = AppConfig(
        paths=PathsConfig(),
        rclone=RcloneSettings(),
        duckdb=DuckDbSettings(),
        ndjson=NdjsonSettings(),
        downloader=DownloaderSettings()
    )
    
    assert config.paths.data_dir == "./extracted_data"
    assert config.rclone.transfers == 100


def test_load_config_from_json():
    """Test loading configuration from JSON file."""
    # Create a temporary config file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        config_data = {
            "Paths": {
                "DataDir": "./test_data",
                "ParquetDir": "./test_parquet"
            },
            "Rclone": {
                "RemoteBase": "test:bucket",
                "Transfers": 50
            }
        }
        json.dump(config_data, f)
        temp_path = f.name
    
    try:
        config = AppConfig.load(temp_path)
        assert config.paths.data_dir == "./test_data"
        assert config.paths.parquet_dir == "./test_parquet"
        assert config.rclone.remote_base == "test:bucket"
        assert config.rclone.transfers == 50
    finally:
        os.unlink(temp_path)


def test_load_config_nonexistent_file():
    """Test loading config from nonexistent file returns defaults."""
    config = AppConfig.load("/nonexistent/config.json")
    assert config.paths.data_dir == "./extracted_data"


if __name__ == "__main__":
    import pytest
    pytest.main([__file__, "-v"])
