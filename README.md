# Movie Market Analysis using PySpark & Databricks

![Python](https://img.shields.io/badge/Python-3.9-blue?style=flat-square)
![PySpark](https://img.shields.io/badge/PySpark-3.x-orange?style=flat-square)
![Databricks](https://img.shields.io/badge/Platform-Databricks-red?style=flat-square)
![Domain](https://img.shields.io/badge/Domain-Media%20%26%20Entertainment-purple?style=flat-square)
![Status](https://img.shields.io/badge/Status-Complete-brightgreen?style=flat-square)

---

## Overview

A distributed data analysis project exploring **10 years of IMDB movie market data (2006–2016)** using Apache PySpark on Databricks Community Edition. The project simulates a real-world big data analytics workflow — from raw data ingestion and cleaning through to business-relevant insights on revenue trends, genre performance, and rating distributions.

The core motivation: understanding what actually drives box office success — is it genre, year, critic scores, or something else entirely?

---

## Business Questions Answered

| # | Question | Technique Used |
|---|----------|----------------|
| 1 | Which movies ranked in the top 3 by revenue within each year? | Window Functions — `rank()` |
| 2 | Which genres generate the highest and lowest total revenue? | Column exploding + `groupBy` aggregation |
| 3 | How has total box office revenue trended year over year? | Year-wise `groupBy` + `sum()` |
| 4 | Which films are the highest and lowest rated on IMDB? | `max()` / `min()` with tie-safe filtering |
| 5 | Who directed the lowest revenue film of 2009? | Filtered aggregation |
| 6 | How do Action, Adventure, Fantasy blockbusters perform as a group? | Genre-specific filtering |
| 7 | What are the top 10 revenue films per year? | Window Functions + Spark SQL Temp View |

---

## Dataset

| Property | Detail |
|----------|--------|
| Source | IMDB Movie Data — Kaggle |
| Time Period | 2006 – 2016 |
| Records | 1,000 movies |
| Columns | 12 (Title, Year, Genre, Rating, Votes, Revenue, Metascore, Director, Runtime, etc.) |
| Format | CSV (with complex quoting and multi-line fields) |
| Key Challenge | Missing values in Revenue and Metascore; Genre stored as comma-separated multi-value strings |

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| **Apache PySpark** | Distributed DataFrame processing and SQL |
| **Databricks Community Edition** | Cluster-based Spark execution environment |
| **PySpark Window Functions** | Year-wise ranking and partitioned analysis |
| **Spark SQL** | SQL queries via temporary views |
| **Python 3.9** | Scripting and function orchestration |

---

## Project Workflow

```
Raw CSV (IMDB Data)
        │
        ▼
┌─────────────────────┐
│   1. Data Loading   │  ← Handles multiLine CSV, quoted fields, whitespace
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  2. Type Casting    │  ← Explicit casting of all numeric columns
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  3. Data Quality    │  ← Duplicate check, isnull() + isnan() null audit
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  4. Cleaning        │  ← Mean imputation for Revenue & Metascore nulls
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  5. Standardisation │  ← Round ratings to 1 decimal for consistency
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  6. EDA & Analysis  │  ← Window functions, groupBy, explode, Spark SQL
└─────────────────────┘
        │
        ▼
┌─────────────────────┐
│  7. Key Findings    │  ← Business insights and market patterns
└─────────────────────┘
```

---

## Key Technical Highlights

### Window Functions
Used PySpark's `Window` API to rank movies by revenue within each year without collapsing the DataFrame — preserving all movie details while adding a partitioned rank column.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

windowSpec = Window.partitionBy("Year").orderBy(col("Revenue (Millions)").desc())
df_ranked = df.withColumn("rank", rank().over(windowSpec))
```

`rank()` was specifically chosen over `row_number()` to correctly handle ties — if two films share the same revenue in a year, both receive the same rank rather than arbitrarily assigning a single winner.

---

### Genre Exploding
The Genre column stores multi-genre values as a single comma-separated string (e.g., `"Action,Adventure,Fantasy"`). Treating these as unique combinations would incorrectly isolate genres. The explode approach splits each multi-genre record into individual rows for accurate per-genre aggregation.

```python
from pyspark.sql.functions import explode, split, trim

df_genres = df.withColumn("Genre", explode(split(col("Genre"), ",")))
df_genres = df_genres.withColumn("Genre", trim(col("Genre")))
```

---

### Null Handling — isnull() + isnan() Combined
Float columns can contain both Python `None` (caught by `isnull()`) and IEEE `NaN` (caught by `isnan()`). Using only one misses half the missing data. The combined approach ensures a complete null audit.

```python
missing_counts = df.select([
    count(when(isnull(c) | isnan(c), c)).alias(c)
    for c in df.columns
])
```

---

### Spark SQL Integration via Temp View
Demonstrates production-style Databricks workflow by registering a ranked DataFrame as a SQL-queryable temporary view.

```python
top_10_movies.createOrReplaceTempView("Top_10_Movies_Per_Year")

spark.sql("""
    SELECT Year, Title, `Revenue (Millions)`, rank
    FROM Top_10_Movies_Per_Year
    ORDER BY Year, rank
""").display()
```

---

## Key Findings

- **Revenue trend:** Box office revenue shows consistent growth across 2006–2016, with 2015–2016 representing peak years in the dataset
- **Genre dominance:** Action and Adventure genres consistently lead in total revenue; niche genres like Documentary and Music generate significantly less
- **Rating vs Revenue:** High IMDB ratings do not reliably predict high revenue — critically acclaimed films frequently underperform commercially relative to mainstream blockbusters
- **Blockbuster clustering:** The Action, Adventure, Fantasy combination — characteristic of superhero and fantasy franchise films — appears most frequently in top revenue positions
- **Year-wise competition:** Revenue concentration in the top 3 films per year increased over the decade, suggesting growing blockbuster dominance at the expense of mid-budget films

---

## How to Run

> This notebook was built and executed on **Databricks Community Edition** using a managed Spark cluster. It requires a Spark environment to run.

**To run on Databricks:**
1. Import the `.ipynb` file into your Databricks workspace
2. Upload the IMDB dataset to your Databricks FileStore
3. Update the file path in Cell 1 to match your FileStore location
4. Attach to a cluster and run all cells

**To run locally:**
1. Install PySpark: `pip install pyspark`
2. Add a SparkSession initialisation cell at the top:
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MovieMarketAnalysis").getOrCreate()
```
3. Replace the FileStore path with your local CSV path
4. Run the notebook in Jupyter or VS Code

---

## Repository Structure

```
movie-market-analysis-pyspark/
│
├── movie_market_analysis_pyspark.ipynb   ← Main analysis notebook (15 sections)
└── README.md                             ← Project documentation
```

---

## Skills Demonstrated

- **Distributed data processing** using Apache PySpark DataFrame API
- **Window Functions** — partitioned ranking with tie-safe `rank()`
- **Data cleaning** — type casting, duplicate removal, combined null detection, mean imputation
- **Column transformation** — exploding multi-value string columns for accurate aggregation
- **Spark SQL** — temp view creation and SQL querying on DataFrames
- **Databricks environment** — FileStore, cluster-based execution, `.display()` rendering
- **EDA methodology** — structured analysis flow from raw data to business insight

---

## About This Project

This project was completed as part of the Advanced PG Program in Data Science with AI & ML at **Vishwakarma Institute of Technology (VIT), Pune** under the guidance of the course instructor. It was executed on Databricks Community Edition using a shared Spark cluster.

The dataset covers the global movie market across a decade and was chosen for its real-world messiness — complex CSV formatting, missing values, and multi-value genre strings — making it a practical exercise in production-style data engineering and analysis.

---

## Connect

**Shreya Vijay Jadhav**  
Data Analyst | SQL · Python · Power BI · PySpark · Media & OTT Analytics  

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?style=flat-square&logo=linkedin)](https://linkedin.com/in/shreyavijayjadhav)
[![GitHub](https://img.shields.io/badge/GitHub-Profile-black?style=flat-square&logo=github)](https://github.com/shreyavijayjadhav3)
