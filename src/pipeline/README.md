# Pipeline Orchestration

This directory contains the orchestration modules for the GitHub Event Intelligence Pipeline.

## Components

### ingest_pipeline.py

Coordinates the data ingestion process, including:
- Downloading data from GitHub Archive
- Uploading raw data to S3
- Initial processing through the bronze layer

### medalion_pipeline.py

Orchestrates the full Medallion Architecture data flow:
- Bronze layer processing (raw data validation and initial structuring)
- Silver layer processing (data normalization and cleaning)
- Gold layer processing (business-level aggregations and metrics)
- Loading data into Snowflake for analytics

## Usage

To run the full pipeline:

```bash
# Run the ingestion pipeline (download and upload to S3)
python -m src.pipeline.ingest_pipeline

# Run the Medallion architecture pipeline (bronze, silver, gold transformations)
python -m src.pipeline.medalion_pipeline
```

## Configuration

The pipeline uses configuration from:
- `src/config/pipeline_config.yaml` - General pipeline settings
- `src/config/snowflake_config.py` - Snowflake connection parameters

## Dependencies

The pipeline depends on modules in:
- `src/data_flow/download` - For downloading data
- `src/data_flow/data_preprocessing` - For the Medallion architecture
- `src/data_flow/storage` - For S3 and Snowflake operations