import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class PathsConfig:
    data_dir: str = "./extracted_data"
    parquet_dir: str = "./parquet_data"
    output_dir: str = "./cnpj_ndjson"
    download_dir: str = "./downloads"
    hash_cache_dir: str = "./hash_cache"


@dataclass
class RcloneSettings:
    remote_base: str = ""
    transfers: int = 100
    max_concurrent_uploads: int = 4


@dataclass
class DuckDbSettings:
    use_in_memory: bool = True
    threads_pragma: int = 2
    memory_limit: str = "5GB"
    engine_threads: int = 2
    preserve_insertion_order: bool = False


@dataclass
class NdjsonSettings:
    batch_upload_size: int = 10000
    normalize_before_hash: bool = False
    write_json_files: bool = False
    max_parallel_processing: int = 8


@dataclass
class DownloaderSettings:
    parallel_downloads: int = 6


@dataclass
class AppConfig:
    paths: PathsConfig
    rclone: RcloneSettings
    duckdb: DuckDbSettings
    ndjson: NdjsonSettings
    downloader: DownloaderSettings

    @classmethod
    def load(cls, path: Optional[str] = None) -> "AppConfig":
        """Load configuration from JSON file."""
        config_path = path or os.path.join(os.getcwd(), "config.json")
        
        try:
            if os.path.exists(config_path):
                with open(config_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                
                return cls(
                    paths=PathsConfig(**{k.lower().replace("dir", "_dir"): v for k, v in data.get("Paths", {}).items()}),
                    rclone=RcloneSettings(**{k.lower().replace("base", "_base"): v for k, v in data.get("Rclone", {}).items()}),
                    duckdb=DuckDbSettings(**{
                        "use_in_memory": data.get("DuckDb", {}).get("UseInMemory", True),
                        "threads_pragma": data.get("DuckDb", {}).get("ThreadsPragma", 2),
                        "memory_limit": data.get("DuckDb", {}).get("MemoryLimit", "5GB"),
                        "engine_threads": data.get("DuckDb", {}).get("EngineThreads", 2),
                        "preserve_insertion_order": data.get("DuckDb", {}).get("PreserveInsertionOrder", False),
                    }),
                    ndjson=NdjsonSettings(**{
                        "batch_upload_size": data.get("Ndjson", {}).get("BatchUploadSize", 10000),
                        "normalize_before_hash": data.get("Ndjson", {}).get("NormalizeBeforeHash", False),
                        "write_json_files": data.get("Ndjson", {}).get("WriteJsonFiles", False),
                        "max_parallel_processing": data.get("Ndjson", {}).get("MaxParallelProcessing", 8),
                    }),
                    downloader=DownloaderSettings(**{
                        "parallel_downloads": data.get("Downloader", {}).get("ParallelDownloads", 6)
                    })
                )
        except Exception:
            pass
        
        # Return default config if loading fails
        return cls(
            paths=PathsConfig(),
            rclone=RcloneSettings(),
            duckdb=DuckDbSettings(),
            ndjson=NdjsonSettings(),
            downloader=DownloaderSettings()
        )


# Global config instance
_current_config: Optional[AppConfig] = None


def get_config() -> AppConfig:
    """Get the current configuration instance."""
    global _current_config
    if _current_config is None:
        _current_config = AppConfig.load()
    return _current_config


def set_config(config: AppConfig) -> None:
    """Set the current configuration instance."""
    global _current_config
    _current_config = config
