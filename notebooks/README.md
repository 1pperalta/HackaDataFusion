# Jupyter Notebooks

This directory contains Jupyter notebooks for exploratory data analysis (EDA), data visualization, and prototyping of analytical models for the GitHub Event Intelligence Pipeline.

## Notebooks

- **GitHub_Data_EDA.ipynb**: Exploratory analysis of GitHub events data
- **User_Activity_Analysis.ipynb**: Analysis of user activity patterns
- **Repository_Trends.ipynb**: Analysis of repository trends over time
- **Anomaly_Detection.ipynb**: Prototype for detecting unusual activity patterns
- **Language_Usage_Trends.ipynb**: Analysis of programming language adoption trends

## How to Use

1. Make sure you have Jupyter installed:
```bash
poetry install
```

2. Start Jupyter Lab:
```bash
poetry run jupyter lab
```

3. Navigate to the desired notebook and run the cells

## Dependencies

These notebooks rely on data processed through the pipeline and stored in either local files or Snowflake. Ensure that you have:

1. Downloaded raw data from GitHub Archive
2. Processed it through the pipeline
3. Configured Snowflake access if using warehoused data

## Data Visualization

These notebooks generate visualizations that can be exported for dashboards or further analysis. Key visualizations include:

- Time series of GitHub events
- Geographic distribution of users
- Repository popularity metrics
- Language usage trends
- Anomaly detection results

## Best Practices

- Keep notebooks focused on a single analysis task
- Document your findings with markdown cells
- Include explanations of methodology and insights
- Export finished visualizations to the `docs` folder for reference