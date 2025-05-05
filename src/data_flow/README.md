# Data Flow

This directory contains the modules responsible for moving data through the pipeline, from extraction to loading and transformation.

## Components

### data_preprocessing/

This folder implements the Medallion Architecture with three layers:

- **Bronze Processor (`bronze_processor.py`)**: Handles raw data from GitHub Archive with minimal transformations
- **Silver Processor (`silver_processor.py`)**: Cleans, normalizes, and prepares data for analytics
- **Gold Processor (`gold_processor.py`)**: Produces aggregated, business-ready datasets

#### Silver Layer Tables

The Silver layer normalizes raw GitHub event data into the following dimensional tables:

- **`events`**: Core event facts with identifiers, timestamps, and references
  - Columns: event_id, event_hash, event_type, created_at, actor_id, repo_id, org_id, is_bot, public, hour_bucket, processed_at
  
- **`actors`**: GitHub user information
  - Columns: actor_id, actor_login, actor_display_login, actor_url, actor_type, actor_site_admin, avatar_url, gravatar_id, first_seen_at, last_seen_at
  
- **`repositories`**: Repository metadata and statistics
  - Columns: repo_id, repo_name, repo_url, owner_id, owner_login, is_fork, language, created_at, updated_at, first_seen_at, last_seen_at, stars_count, forks_count, issues_count, watchers_count, size
  
- **`organizations`**: Organization information
  - Columns: org_id, org_login, org_url, avatar_url, description, first_seen_at, last_seen_at
  
- **`payload_details`**: Event-specific payload information
  - Columns: event_id, event_type, payload_action, payload_issue_id, payload_pull_request_id, payload_comment_id, payload_push_size, payload_ref, payload_ref_type, payload_head, payload_before, payload_size, payload_distinct_size

#### Gold Layer Tables

The Gold layer provides business-ready aggregated metrics:

- **`actor_metrics`**: Activity metrics for GitHub users
  - Includes: total_events, unique_repos, first_event, last_event, plus actor attributes
  
- **`repo_metrics`**: Repository engagement metrics
  - Includes: total_events, unique_actors, first_event, last_event, plus repository attributes
  
- **`org_metrics`**: Organization activity metrics
  - Includes: total_events, unique_actors, first_event, last_event, plus organization attributes
  
- **`event_type_metrics`**: Distribution of events by type
  - Includes: event counts grouped by event_type
  
- **`daily_summary`**: Time-based activity summaries
  - Includes: total_events, unique_actors, unique_repos aggregated by hour_bucket


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
