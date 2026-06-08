# 🚕 NYC Taxi Fare Prediction — Spark & Databricks

A big data engineering project analysing **~974 million NYC taxi trips (2014–2024)** using Apache Spark on Databricks. The project covers the full data pipeline — from raw ingestion to machine learning — and uncovers actionable business insights about fare patterns, demand trends, and driver earnings.

---

## 🔍 Project Overview

New York City's yellow and green taxi fleet generates one of the world's largest urban mobility datasets. This project uses that data to:

- Build a scalable **data pipeline** on Databricks with Delta Lake
- Answer **11 business questions** through Spark SQL EDA
- Train and compare **4 machine learning models** to predict total trip fare
- Derive **actionable insights** for drivers, operators, and city planners

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
| Matplotlib             | Visualisation                                       |

---

## 🗂️ Project Structure

```
nyc-taxi-fare-prediction-spark/
│
├── notebooks/              # Databricks notebooks (exported as .py)
│   ├── Part_1_Cleaning.py
│   ├── Part_2_Queries.py
│   ├── Part_3_EDA.py
│   └── Part_4_Modelling.py
│
├── report/
│   └── Report.pdf
│
└── README.md
```

---

## 🔄 Pipeline

```
Raw Parquet Files (Yellow + Green)
        ↓
  Data Cleaning (7 rules)
        ↓
  Schema Alignment + Union
        ↓
  Geographic Enrichment (TLC Taxi Zones)
        ↓
  Delta Lake (trips_final — 974M rows)
        ↓
  Spark SQL EDA (11 Business Questions)
        ↓
  Stratified Sampling (4%) → Train/Val/Test Split
        ↓
  ML Modelling (Linear, Ridge, SGD, MLP)
```

---

## 🤖 Model Results

| Model                   | Test RMSE |
| ----------------------- | --------- |
| Baseline (avg lookup)   | 103.01    |
| Linear Regression       | 37.13     |
| Ridge Regression        | 3,560.05  |
| SGD (Huber loss)        | 89,935.65 |
| **MLP (64, 32) — Best** | **13.56** |

The **MLP neural network** achieved the best performance, reducing test RMSE by ~87% vs the baseline.

---

## 📦 Dataset

Data sourced from the [NYC Taxi and Limousine Commission (TLC)](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

- **Yellow Taxi:** ~908M trips (2014–2024)
- **Green Taxi:** ~83M trips (2014–2024)
- **Final cleaned dataset:** ~974M rows stored in Delta format

---

## ⚠️ Challenges

- Working within the **free Databricks tier (32GB RAM)** required careful multi-stage sampling
- Full dataset training was infeasible; a stratified **4% sample (~39M rows)** was used, further reduced to **~1.55M rows** for Pandas-based ML
- Sessions frequently shut down under memory pressure, requiring step-by-step data loading

---

## 👩‍💻 Author

**Monika Shakya**  
Master of Data Science and Innovation  
University of Technology Sydney
