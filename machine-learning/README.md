# Machine Learning Workflow
# ✈️ Flight Delay Prediction using Apache Spark MLlib

## 📌 Project Overview

This project predicts whether a flight will be delayed by **15 minutes or more (ArrDel15)** using Apache Spark MLlib on Amazon EMR. The project focuses on scalable machine learning for large datasets by leveraging distributed data processing and feature engineering.

The complete workflow includes data preprocessing, feature engineering, feature selection, model training, and evaluation using Random Forest.

---

## 📂 Dataset

- **Source:** Airline Flight Delay Dataset (2020–2025)
- **Storage:** Amazon S3 (Gold Layer)
- **Format:** Apache Parquet
- **Records:** ~40 Million
- **Target Variable:** `ArrDel15`

---

## 🛠️ Tech Stack

- Python
- Apache Spark (PySpark)
- Spark MLlib
- Amazon EMR
- Amazon S3
- Jupyter Notebook

---

## 📊 Features Used

### Numerical Features

- DepartureHour
- ArrivalHour
- PeakHourIndicator
- WeekendIndicator
- Distance
- ScheduledElapsedTimeMinutes
- CodeshareFlag
- IntraStateRouteFlag
- AirlineReliabilityScore
- OriginAirportReliabilityScore
- DestAirportReliabilityScore
- RouteReliabilityScore

### Categorical Features

- MarketingAirlineKey
- OperatingAirlineKey
- DeparturePeriod
- ArrivalPeriod
- SeasonIndicator
- DistanceCategory

---

## 🔄 Project Workflow

```
Raw Parquet Data (Amazon S3)
            │
            ▼
      Data Cleaning
            │
            ▼
 Handle Missing Values
            │
            ▼
 Feature Engineering
            │
            ▼
 String Indexing
            │
            ▼
 Vector Assembler
            │
            ▼
 Feature Selection
 (Random Forest Importance)
            │
            ▼
 Selected Features
            │
            ▼
 Random Forest Training
            │
            ▼
 Model Evaluation
```

---

## ⚙️ Preprocessing

- Removed unnecessary metadata columns
- Handled missing values
- Indexed categorical features using `StringIndexer`
- Combined numerical and categorical features using `VectorAssembler`

---

## 🌲 Feature Selection

Feature importance was extracted using **Random Forest** trained on a sampled dataset.

Selected Features:

- RouteReliabilityScore
- DepartureHour
- DeparturePeriod
- SeasonIndicator
- ArrivalHour
- AirlineReliabilityScore
- ArrivalPeriod
- OriginAirportReliabilityScore
- OperatingAirlineKey
- MarketingAirlineKey
- DestAirportReliabilityScore
- CodeshareFlag

---

## 🤖 Model

Algorithm:

- Random Forest Classifier

Hyperparameters:

- Number of Trees: 100
- Maximum Depth: 10
- Maximum Bins: 64
- Feature Subset Strategy: sqrt

---

## 📈 Evaluation Metrics

The model was evaluated using:

- Accuracy
- Precision
- Recall
- F1 Score
- ROC-AUC

---

## 🚀 Project Structure

```
Flight-Delay-Prediction/
│
├── notebooks/
│   └── flight_ml_rf.ipynb
│
├── README.md
│
└── requirements.txt
```

---

## ☁️ Infrastructure

- Amazon EMR
- Apache Spark
- Amazon S3
- Distributed Processing using PySpark

---

## Future Improvements

- Hyperparameter tuning
- Compare Random Forest with GBTClassifier and Logistic Regression
- Model deployment using SageMaker or ECS
- CI/CD using GitHub Actions
- Real-time prediction pipeline

---

## Author

Dharmraj Patil

Machine Learning | Data Engineering | AWS | Apache Spark
