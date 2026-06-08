# 🚕 NYC Taxi Fare Prediction — Spark & Databricks

A big data engineering project analysing **~974 million NYC taxi trips (2014–2024)** using Apache Spark on Databricks. The project covers the full data pipeline — from cleaning and enrichment to exploratory analysis and machine learning — and uncovers actionable business insights about fare patterns, demand trends, and driver earnings.

---

## 🔍 Project Overview

New York City's yellow and green taxi fleet generates one of the world's largest urban mobility datasets. This project uses that data to:

- Build a scalable **data cleaning and enrichment pipeline** on Databricks with Delta Lake
- Answer **business questions** through Spark SQL EDA
- Train and compare **4 machine learning models** to predict total trip fare
- Derive **actionable insights** for drivers, operators, and city planners

> **Note:** Raw Parquet files (Yellow + Green) are pre-loaded into a Databricks Volume. There is no separate ingestion notebook — `Part_1_Cleaning.py` is the pipeline entry point.

---

## 📊 Key Insights

- **Peak demand** consistently occurs at **7 PM**, with Fridays and Saturdays the busiest days
- **Manhattan → Manhattan** trips account for **62.6% of all taxi revenue** in 2024
- **63%** of trips included tips, but only 0.83% of tipped trips exceeded $15
- Trips lasting **60+ minutes** yield the highest average revenue (~$70/trip) and best km-per-dollar value
- Green taxis travel **faster on average** (~20.5 km/h vs 18.9 km/h) despite similar trip distances to yellow taxis
- Spring months (March–May) see the **highest trip volumes**, suggesting clear seasonal demand patterns

---

## 🛠️ Tech Stack

| Tool                   | Purpose                                             |
| ---------------------- | --------------------------------------------------- |
| Apache Spark (PySpark) | Distributed data processing                         |
| Databricks             | Cloud compute & notebook environment                |
| Delta Lake             | Storage format with schema enforcement & versioning |
| Spark SQL              | Exploratory data analysis & business queries        |
| Python / Pandas        | ML modelling (scikit-learn)                         |
| Matplotlib / Seaborn   | Visualisation                                       |

---

## 🗂️ Project Structure

```
nyc-taxi-analytics-databricks/
│
├── Part_1_Cleaning.py          # Data cleaning, schema alignment, geographic enrichment → Delta Lake
├── Part_2_Queries.py           # Spark SQL business queries (EDA)
├── Part_3_EDA.py               # Correlation analysis and feature exploration
├── Part_4_Modelling.py         # Stratified sampling, baseline, and ML modelling
│
├── Part_1_Cleaning.html        # Exported notebook outputs
├── Part_2_Queries.html
├── Part_3_EDA.html
├── Part_4_Modelling.html
│
├── linear_model_0.5.pkl        # Saved trained models
├── ridge_model_0.5.pkl
├── sgd_model_0.5.pkl
├── mlp_model_0.5.pkl
│
├── 94693-AT2-25548660-project_report.pdf
└── README.md
```

---

## 🔄 Pipeline

```
Raw Parquet Files (Yellow + Green) — pre-loaded into Databricks Volume
        ↓
  Data Cleaning (7 rules — see Part_1_Cleaning.py)
        ↓
  Schema Alignment + Union (yellow_clean + green_clean)
        ↓
  Geographic Enrichment (TLC Taxi Zone lookup → pu/do borough & zone)
        ↓
  Delta Lake (workspace.bde.trips_final — ~974M rows)
        ↓
  Spark SQL EDA (Part_2_Queries.py — demand, revenue, speed, tips, duration bins)
        ↓
  Stratified Sampling (4% overall) → 5% Pandas sub-sample → ~1.55M rows
        ↓
  Train / Val / Test Split (temporal: test = Oct–Dec 2024)
        ↓
  ML Modelling (Linear, Ridge, SGD, MLP)
```

### Cleaning Rules

| # | Rule |
|---|------|
| 1 | Drop trips where `dropoff_ts < pickup_ts` |
| 2 | Drop trips outside date range 2014–2024 |
| 3 | Drop trips with negative speed |
| 4 | Drop trips with speed > 150 km/h |
| 5 | Drop trips with duration < 60s or > 6 hours |
| 6 | Drop trips with distance < 0.2 km or > 200 km |
| 7 | Drop trips with fare below minimum ($3.30) unless payment type is cancelled/voided |

---

## 🤖 Model Results

| Model                   | Test RMSE |
| ----------------------- | --------- |
| Baseline (avg lookup)   | 103.01    |
| Linear Regression       | 37.13     |
| Ridge Regression        | 3,560.05  |
| SGD (Huber loss)        | 89,935.65 |
| **MLP (64, 32) — Best** | **13.56** |

The **MLP neural network** achieved the best performance, reducing test RMSE by ~87% vs the baseline. All models use a `log1p` target transformation and a preprocessing pipeline with median imputation, standard scaling for numeric features, and one-hot encoding for categoricals.

---

## 📦 Dataset

Data sourced from the [NYC Taxi and Limousine Commission (TLC)](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

- **Yellow Taxi:** ~908M trips (2014–2024)
- **Green Taxi:** ~83M trips (2014–2024)
- **Final cleaned dataset:** ~974M rows stored in Delta format

---

## ⚠️ Challenges

- Working within the **free Databricks tier (32GB RAM)** required careful multi-stage sampling
- Full dataset training was infeasible; a stratified **4% sample (~39M rows)** was used, then further reduced via a **5% Pandas sub-sample (~1.55M rows)** for scikit-learn ML
- Sessions frequently shut down under memory pressure, requiring step-by-step data loading
- Thread contention on the Databricks cluster required explicit thread limits (`threadpoolctl`, `OPENBLAS_NUM_THREADS=1`)

---

## 👩‍💻 Author

**Monika Shakya**  
Master of Data Science and Innovation  
University of Technology Sydney
