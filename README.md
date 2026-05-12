# safra-brasil-analytics

Analytics project for Brazilian harvest data analysis, using Google BigQuery as the data warehouse and Python for data ingestion and transformation.

## Stack

- **Cloud:** Google Cloud Platform (BigQuery)
- **Language:** Python 3.11+
- **Libraries:** google-cloud-bigquery, pandas, pyarrow, db-dtypes, requests, openpyxl
- **SQL:** BigQuery Standard SQL (staging and gold layers)
- **Notebooks:** Jupyter
- **Version Control:** Git + GitHub

## Project Structure

```
safra-brasil-analytics/
├── config/          # Project constants and configuration
├── data/
│   ├── raw/         # Raw data files (git-ignored)
│   └── external/    # External reference data
├── src/             # Python source code
├── sql/
│   ├── staging/     # Staging layer queries
│   └── gold/        # Gold layer queries
├── notebooks/       # Exploratory analysis
├── dashboards/      # Dashboard files
├── docs/
│   └── screenshots/ # Documentation screenshots
└── requirements.txt
```

## Links

- Jira: <!-- project link --> Disponível em breve
- Confluence: <!-- documentation link --> Disponível em breve
- BigQuery Console: <!-- GCP project link --> Disponível em breve
