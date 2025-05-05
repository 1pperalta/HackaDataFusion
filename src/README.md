# Source Code

This directory contains the core components of the GitHub Event Intelligence Pipeline.

## Directory Structure

- **config/**: Configuration files for the pipeline, Snowflake connections, and other components
- **data_flow/**: Modules for data movement and transformation
  - **data_preprocessing/**: Medallion architecture implementation (bronze, silver, gold layers)
  - **download/**: Utilities for downloading GitHub Archive data
  - **storage/**: Modules for S3 and Snowflake data storage operations
  - **models/**: Model creation, testing and monitoring
- **dbt/**: DBT models and transformations for analytical data modeling
- **pipeline/**: Pipeline orchestration modules
- **scripts/**: Utility scripts for data catalog and validation
- **utils/**: Common utility functions used across the project

## Usage

The code in this directory is organized to support the complete data pipeline flow:

1. Data ingestion from GitHub Archive
2. Storage in AWS S3
3. Processing through the Medallion Architecture layers
4. Transformation using dbt
5. Loading to Snowflake for analytics

Please refer to individual module READMEs for detailed documentation.