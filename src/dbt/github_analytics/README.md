# GitHub Analytics dbt Project

This directory contains the dbt (data build tool) models that transform the GitHub event data through the Medallion Architecture.

## Project Structure

- **models/**: Contains the core SQL models for data transformation
  - **silver/**: Silver layer models that clean and normalize the raw data
    - `silver_actors.sql`: Processes actor-related data
    - `silver_events.sql`: Processes event data
    - `silver_organizations.sql`: Processes organization data
    - `silver_payload_details.sql`: Processes event payload details
    - `silver_repositories.sql`: Processes repository data
  - **gold/**: Gold layer models that provide business-level aggregations
    - `gold_actor_metrics.sql`: Metrics related to GitHub actors (users)
    - `gold_daily_summary.sql`: Daily aggregated metrics
    - `gold_event_type_metrics.sql`: Metrics per event type
    - `gold_org_metrics.sql`: Organization-level metrics
    - `gold_repo_metrics.sql`: Repository-level metrics
- **macros/**: Custom SQL functions and utilities
- **seeds/**: Static data files for reference data
- **snapshots/**: Slowly changing dimension snapshots
- **analyses/**: SQL analyses that are not materialized
- **tests/**: Custom data tests
- **data/**: Stores raw data for dbt

## Usage

### Running dbt Models

To run all models:
```bash
dbt run
```

To run specific layers:
```bash
dbt run --select silver
dbt run --select gold
```

### Testing

To test all models:
```bash
dbt test
```

### Generate Documentation

```bash
dbt docs generate
dbt docs serve
```

## Dependencies

The gold models depend on silver models, which in turn depend on the raw data imported from S3 to Snowflake. Make sure data flows through the pipeline properly before running these models.

## Configuration

This project uses the following configuration files:
- `dbt_project.yml`: Main project configuration
- `profiles.yml`: Connection profile for Snowflake
- `schema.yml` files: Column-level documentation and tests

### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
