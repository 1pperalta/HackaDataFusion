# 📄 Product Requirements Document (PRD)

## 🏷 Project Name: GitHub Event Intelligence Pipeline  
**Team:** 3 Members (Data Engineers & Data Scientists)  
**Hackathon:** GDC Fusion | Wizeline Hackathon 2025  
**Sponsor:** Wizeline  
**Duration:** 1–2 Days  

---

## 🎯 Objective

Design and implement a complete data pipeline to process, analyze, and visualize GitHub Archive (GHArchive) event data. The goal is to extract meaningful insights about the open-source ecosystem, such as trending repositories, language adoption trends, user activity patterns, and anomalies.

---

## 🧩 Key Deliverables

### ✅ Data Engineering Track

1. **Ingest Raw GitHub Data**
   - Download `.json.gz` GitHub event files from [https://www.gharchive.org/](https://www.gharchive.org/) for 2–7 days.
   - Store them in **AWS S3** (using AWS Educate accounts).

2. **Transform & Enrich**
   - Use **DBT** and **Medallion Architecture** (Bronze → Silver → Gold) for data modeling.
   - Optional Bonus: Integrate **Mage.ai** or **Apache Spark** for enhanced performance.

3. **Load to Snowflake**
   - Set up a **Snowflake** instance to ingest enriched data.
   - Use it as the central warehouse for analytics and dashboards.

4. **Code Quality**
   - Python project managed with **Poetry**.
   - Follow clean code practices: modular structure, docstrings, typing, and exception handling.
   - Include testing (e.g., `pytest`) and linting tools (`black`, `isort`, `mypy`).

---

### ✅ Data Science & Analysis Track

1. **Exploratory Data Analysis (EDA)**
   - Analyze user and repo activity trends.
   - Identify high-activity time zones, languages, and regions.

2. **Clustering & Anomaly Detection**
   - Use **KMeans** or **DBSCAN** to group similar users/repos.
   - Apply anomaly detection to flag suspicious behavior (bots, spam).

3. **Predictive Modeling**
   - Use **regression or time series models** to forecast future activity:
     - Active repositories
     - Emerging programming languages
     - Market trends

4. **Visualization & Dashboarding**
   - Use tools like **Metabase**, **Looker Studio**, or **Apache Superset**.
   - Build intuitive dashboards telling a compelling data story:
     - Top trending repos
     - Influential users (excluding bots)
     - Regional/language breakdowns over time

---

## 🧪 Tools & Technologies

| Layer         | Tools / Stack                          |
|--------------|-----------------------------------------|
| Ingestion     | Python, `requests`, `boto3`, `tqdm`     |
| Storage       | **AWS S3** (configured bucket)          |
| Transformation| **DBT**, **Medallion Architecture**     |
| Orchestration | Optional: **Mage.ai**, **Spark**        |
| Warehousing   | **Snowflake**                           |
| Modeling      | `pandas`, `scikit-learn`, `Prophet`     |
| Visualization | Metabase, Looker Studio, Superset       |
| DevOps        | GitHub, Poetry, pytest, black, isort    |

---


---

## 📹 Submission Requirements

- ✅ GitHub repository with all code and documentation
- ✅ `README.md` with setup, usage, and architecture explanation
- ✅ `prd.md` (this file) and `architecture_diagram.png`
- ✅ 2–3 minute **demo video**:
  - Face cam required
  - Clear audio
  - Demo of pipeline and dashboard

---

## 📊 Evaluation Criteria

| Criteria         | Description                                               |
|------------------|-----------------------------------------------------------|
| Performance      | Can handle new incoming files efficiently                 |
| Code Quality     | Modular, clean, tested, documented                        |
| Tool Usage       | Uses DBT, Snowflake, Python; Bonus: Mage/Spark            |
| Insights         | Depth and clarity of analysis and storytelling            |
| Documentation    | README, diagrams, demo clarity                            |


