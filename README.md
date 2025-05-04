# GitHub Event Intelligence Pipeline

![GitHub Event Intelligence](https://img.shields.io/badge/GDC%20Fusion-Hackathon%202025-blue)
![Python](https://img.shields.io/badge/Python-3.9%2B-brightgreen)
![dbt](https://img.shields.io/badge/dbt-Core-orange)
![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-9cf)

## 📋 Overview

The GitHub Event Intelligence Pipeline is a comprehensive data engineering and analytics solution for processing and analyzing GitHub Archive data. This project was developed for the GDC Fusion | Wizeline Hackathon 2025, aiming to extract valuable insights from GitHub event data using modern data engineering practices and analytics techniques.

## 🏗️ Architecture

This project implements a modern data pipeline with the Medallion Architecture:
- **Bronze Layer**: Raw data ingestion from GitHub Archive
- **Silver Layer**: Cleaned and normalized data
- **Gold Layer**: Business-level aggregations and metrics

![Data Pipeline Architecture](docs/architecture_diagram.png)

## 📦 Repository Structure

```
.
├── data/                  # Data storage
│   ├── external/          # External data sources
│   └── processed/         # Processed data files
├── docs/                  # Documentation
├── notebooks/             # Jupyter notebooks for EDA
├── s3_upload/             # S3 upload utilities
├── src/                   # Source code
│   ├── config/            # Configuration files
│   ├── data_flow/         # Data processing modules
│   │   ├── data_preprocessing/ # Medallion architecture processors
│   │   ├── download/      # Data download utilities
│   │   └── storage/       # Storage connection utilities
│   ├── dbt/               # DBT models and transformations
│   ├── pipeline/          # Pipeline orchestration
│   ├── scripts/           # Utility scripts
│   └── utils/             # Common utilities
└── tests/                 # Test suites
```

## 🚀 Getting Started

### Prerequisites

- Python 3.9+
- Poetry for dependency management
- AWS Account (for S3 storage)
- Snowflake Account
- dbt Core

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/HackaDataFusion-2.git
cd HackaDataFusion-2
```

2. Install dependencies using Poetry:
```bash
poetry install
```

3. Configure your environment:
   - Set up AWS credentials
   - Configure Snowflake connection parameters in `src/config/snowflake_config.py`
   - Update dbt profiles in `src/dbt/github_analytics/profiles.yml`

### Running the Pipeline

1. Download data from GitHub Archive:
```bash
poetry run python -m src.data_flow.download.download
```

2. Upload raw data to S3:
```bash
poetry run python -m s3_upload.upload_to_s3
```

3. Run the data processing pipeline:
```bash
poetry run python -m src.pipeline.medalion_pipeline
```

4. Run dbt transformations:
```bash
cd src/dbt/github_analytics
dbt run
```

## 📊 Dashboards and Analysis

After running the pipeline, analytics dashboards can be accessed through your chosen visualization tool:
- Metabase
- Looker Studio
- Apache Superset

Key insights include:
- Trending GitHub repositories
- Activity patterns by language and region
- Anomaly detection for unusual event patterns
- User contribution analysis

## 👥 Team

- [Team Member 1] - Data Engineer
- [Team Member 2] - Data Scientist
- [Team Member 3] - Data Engineer/Scientist

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgements

- [GHArchive](https://www.gharchive.org/) for providing the GitHub events data
- Wizeline for sponsoring the Hackathon
- The open-source community for all the amazing tools that made this project possible