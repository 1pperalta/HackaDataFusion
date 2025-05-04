# Data Flow

This directory contains the modules responsible for moving data through the pipeline, from extraction to loading and transformation.

## Components

### data_preprocessing/

This folder implements the Medallion Architecture with three layers:

- **Bronze Processor (`bronze_processor.py`)**: Handles raw data from GitHub Archive with minimal transformations
- **Silver Processor (`silver_processor.py`)**: Cleans, normalizes, and prepares data for analytics
- **Gold Processor (`gold_processor.py`)**: Produces aggregated, business-ready datasets

### download/

Contains utilities for downloading GitHub Archive data:

- **`download.py`**: Script for downloading `.json.gz` files from GHArchive

### storage/

Modules for interacting with storage systems:

- **`s3_upload.py`**: Handles uploads of raw data to S3
- **`s3_processed_upload.py`**: Manages uploads of processed data to S3
- **`connect_s3_to_snowflake.py`**: Configures and manages the connection between S3 and Snowflake

## Usage

Each module can be run independently or as part of the pipeline:

```bash
# Download GitHub Archive data
python -m src.data_flow.download.download

# Upload to S3
python -m src.data_flow.storage.s3_upload

# Process through bronze layer
python -m src.data_flow.data_preprocessing.bronze_processor

# Process through silver layer
python -m src.data_flow.data_preprocessing.silver_processor

# Process through gold layer
python -m src.data_flow.data_preprocessing.gold_processor

# Connect S3 to Snowflake
python -m src.data_flow.storage.connect_s3_to_snowflake
```

For full pipeline orchestration, please refer to the `src/pipeline` directory.