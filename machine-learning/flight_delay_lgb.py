import joblib
import numpy as np
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OrdinalEncoder
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    classification_report,
    confusion_matrix,
    roc_auc_score,
)
from lightgbm import LGBMClassifier

DATA_PATH = "D:/coding/sample_flight_data/flight_sample_5M.parquet"
MODEL_PATH = "lightgbm_flight_delay.pkl"
ENCODER_PATH = "ordinal_encoder.pkl"
FEATURES_PATH = "feature_columns.pkl"


def load_data(path=DATA_PATH):
    df = pd.read_parquet(path)
    return df


def preprocess_data(df):
    num_cols = df.select_dtypes(include=np.number).columns
    for col in num_cols:
        df[col] = df[col].fillna(df[col].median())

    drop_cols = [
        "FlightDate","Tail_Number","DestAirportKey","OriginAirportKey",
        "MarketingAirlineKey","RouteKey","DatasetSplit","FlightKey"
    ]
    df = df.drop(columns=drop_cols, errors="ignore")

    cat_cols = df.select_dtypes(include="object").columns
    encoder = OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)
    if len(cat_cols):
        df[cat_cols] = encoder.fit_transform(df[cat_cols].astype(str))
    else:
        encoder = None
    return df, encoder


def split_data(df):
    X = df.drop(columns=["ArrDel15"])
    y = df["ArrDel15"]
    return train_test_split(
        X, y, test_size=0.25, random_state=42, stratify=y
    )


def train_model(X_train, y_train):
    model = LGBMClassifier(
        n_estimators=1000,
        learning_rate=0.03,
        num_leaves=70,
        max_depth=-1,
        subsample=0.8,
        colsample_bytree=0.8,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train)
    return model


def evaluate_model(model, X_test, y_test):
    pred = model.predict(X_test)
    prob = model.predict_proba(X_test)[:,1]
    print(f"Accuracy : {accuracy_score(y_test,pred):.4f}")
    print(f"Precision: {precision_score(y_test,pred):.4f}")
    print(f"Recall   : {recall_score(y_test,pred):.4f}")
    print(f"F1 Score : {f1_score(y_test,pred):.4f}")
    print(f"ROC AUC  : {roc_auc_score(y_test,prob):.4f}")
    print("\nClassification Report")
    print(classification_report(y_test,pred))
    print("Confusion Matrix")
    print(confusion_matrix(y_test,pred))


def save_model(model, encoder, feature_columns):
    joblib.dump(model, MODEL_PATH)
    joblib.dump(encoder, ENCODER_PATH)
    joblib.dump(feature_columns, FEATURES_PATH)
    print("Model, encoder and feature list saved.")


def main():
    df = load_data()
    df, encoder = preprocess_data(df)
    X_train, X_test, y_train, y_test = split_data(df)
    model = train_model(X_train, y_train)
    evaluate_model(model, X_test, y_test)
    save_model(model, encoder, list(X_train.columns))


if __name__ == "__main__":
    main()
