## 🤖 Machine Learning Workflow

```text
┌──────────────────────────────┐
│     ✈️ Flight Data           │
│       ~40M Records           │
│   Parquet / Amazon S3        │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│     🧹 Data Preparation      │
│  Cleaning • Validation       │
│  Transformation • ETL        │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│      🥇 Gold ML Dataset      │
│   ML-ready flight records    │
│    Train / Val / Test        │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│   ⚙️ Feature Engineering     │
│  Time • Distance • Route     │
│  Airline • Airport • Season  │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│ 📊 Historical Features       │
│ Reliability Scores           │
│ Delay Rates • Flight Counts  │
│ Monthly / Route Statistics   │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│     🔬 ML Experimentation    │
│  Stratified Sampling         │
│        ↓                     │
│      LightGBM                │
│  Rapid Model Evaluation      │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│    ⚡ Full-Scale Training    │
│         PySpark              │
│    Random Forest Pipeline    │
│  StringIndexer → OHE → RF    │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│       📈 Evaluation          │
│ Accuracy • Precision         │
│ Recall • F1 Score            │
│ Confusion Matrix             │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│     🔍 Explainability        │
│   Feature Importance          │
│       + SHAP Analysis        │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│       ☁️ Model Storage       │
│       Amazon S3              │
│     PipelineModel            │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│       🌐 Deployment          │
│       Streamlit App          │
│                              │
│  ✈️ Delay Prediction         │
│  🏆 Airline Recommendation   │
└──────────────────────────────┘
```

### 📌 Workflow Description

**1. Data Preparation**
Approximately **40 million flight records** are processed using PySpark and stored in Amazon S3. The data undergoes cleaning, validation, transformation and ETL processing.

**2. Feature Engineering**
Flight-level, temporal, route, airline and airport features are created. Historical reliability scores, delay rates, flight counts and monthly statistics are added to capture recurring delay patterns.

**3. ML Experimentation**
A representative/stratified sample is used for rapid experimentation with **LightGBM** before scaling the solution to the complete dataset.

**4. Full-Scale Model**
The final model uses a **PySpark Random Forest pipeline** with categorical encoding and feature assembly, enabling distributed training on the large dataset.

**5. Evaluation & Explainability**
The model is evaluated using Accuracy, Precision, Recall and F1 Score. Feature importance and SHAP analysis are used to understand model behavior.

**6. Deployment**
The complete model pipeline is saved to **Amazon S3** and integrated into a **Streamlit application** for real-time flight delay prediction and airline recommendation.

### 📊 Final Model Performance

| Metric             |      Score |
| ------------------ | ---------: |
| Accuracy           | **79.39%** |
| Weighted Precision | **73.57%** |
| Weighted Recall    | **79.39%** |
| F1 Score           | **70.72%** |

**Final Model:** PySpark Random Forest — `60 trees`, `maxDepth=10`, `maxBins=256`, `featureSubsetStrategy="sqrt"`, and `subsamplingRate=0.8`.
