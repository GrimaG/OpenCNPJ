# ETL_Python - OpenCNPJ ETL Processor (Python Implementation)

Python implementation of the OpenCNPJ ETL processor, converted from the C# .NET version.

## Requirements

- Python 3.10+
- rclone (binary must be installed separately)

## Installation

```bash
pip install -r requirements.txt
```

## Dependencies

- **aiohttp**: Async HTTP client for downloads
- **click**: CLI framework
- **duckdb**: In-memory/file-based SQL database for data processing
- **rich**: Terminal formatting and progress bars
- **xxhash**: Fast hashing algorithm

## Usage

```bash
# Run full pipeline
python -m ETL_Python.main pipeline --month 2024-01

# Process single CNPJ
python -m ETL_Python.main single --cnpj 12345678000190

# Test integrity
python -m ETL_Python.main test

# Generate ZIP
python -m ETL_Python.main zip

# Use custom config
python -m ETL_Python.main --config /path/to/config.json pipeline
```

## Configuration

Create a `config.json` file in the working directory:

```json
{
  "Paths": {
    "DataDir": "./extracted_data",
    "ParquetDir": "./parquet_data",
    "OutputDir": "./cnpj_ndjson",
    "DownloadDir": "./downloads",
    "HashCacheDir": "./hash_cache"
  },
  "Rclone": {
    "RemoteBase": "your-remote:path",
    "Transfers": 100,
    "MaxConcurrentUploads": 4
  },
  "DuckDb": {
    "UseInMemory": true,
    "ThreadsPragma": 2,
    "MemoryLimit": "5GB",
    "EngineThreads": 2,
    "PreserveInsertionOrder": false
  },
  "Ndjson": {
    "BatchUploadSize": 10000,
    "NormalizeBeforeHash": false,
    "WriteJsonFiles": false,
    "MaxParallelProcessing": 8
  },
  "Downloader": {
    "ParallelDownloads": 6
  }
}
```

## Architecture

The Python implementation maintains the same architecture as the C# version:

- **downloaders/**: Web downloader for Receita Federal data
- **processors/**: Data processing (CSV to Parquet, NDJSON export, integrity testing)
- **exporters/**: Rclone client for cloud storage
- **commands/**: CLI command implementations
- **utils/**: Utilities (CNPJ validation, hashing, JSON cleanup)

## Key Differences from C# Version

1. Uses `asyncio` instead of `Task`/`async-await` in .NET
2. Uses `rich` instead of `Spectre.Console`
3. Uses `duckdb` Python package instead of DuckDB.NET
4. Uses `aiohttp` for async HTTP instead of HttpClient
5. Uses `xxhash` Python library instead of xxHash.NET

## Notes

- All SQL queries and business logic are preserved from the C# version
- File structure and naming conventions match the C# implementation
- Type hints are used throughout for better code quality
