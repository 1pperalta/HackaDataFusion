# Jupyter Notebooks

This directory contains Jupyter notebooks for exploratory data analysis (EDA), data visualization, and prototyping of analytical models for the GitHub Event Intelligence Pipeline.

## Notebooks

- **Clustering_gold_no_pca.ipynb**: This notebook evaluates different clustering algorithms on pre-processed repository metrics, without applying dimensionality reduction.

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

## Clustering interpretation
🔹 Cluster 0
repo_id: 0.45 → average repos, slightly above.

total_events: -0.07 → low total activity.

unique_actors: -0.14 → few unique users.

Interpretation: Moderately common repositories, but with low interaction and little reach.

🔹 Cluster 1
repo_id: -1.82 → infrequent repositories.

total_events: -0.03 → very low total activity.

unique_actors: -0.05 → almost no unique users.

Interpretation: Very rarely used repositories, possibly irrelevant or abandoned.

🔹 Cluster 2
repo_id: -0.21 → below-average repos in overall usage.

total_events: 21.20 → extremely high activity.

unique_actors: 0.49 → good number of unique users.

Interpretation: Repos with a lot of activity, possibly maintained by mid-sized teams.

🔹 Group 3
repo_id: -1.57 → uncommon repositories.

total_events: 3.34 → good activity.

unique_actors: 21.97 → many unique users.

Interpretation: Highly collaborative repos with a large number of users, possibly popular public projects.

🔹 Group 4
repo_id: -0.35 → somewhat underrepresented.

total_events: 0.64 → moderate activity.

only_actors: 2.23 → also reasonable interaction.

Interpretation: Repos consistent with frequent users, perhaps growing.

## Best Practices

- Keep notebooks focused on a single analysis task
- Document your findings with markdown cells
- Include explanations of methodology and insights
- Export finished visualizations to the `docs` folder for reference
