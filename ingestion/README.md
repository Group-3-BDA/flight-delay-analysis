# Ingestion Module

## Purpose

This module is responsible for downloading the airline dataset from Kaggle and uploading the raw CSV files to the Bronze S3 bucket.

## Input

- Kaggle Dataset (ZIP)

## Output

- Raw CSV files in Bronze Bucket

## Main Responsibilities

- Download dataset
- Stream ZIP contents
- Upload to S3 Bronze
- Validate uploads
- Cleanup temporary files

## Files

config.py
logger.py
kaggle_client.py
zip_processor.py
s3_uploader.py
validator.py
cleanup.py
main.py

## Team

Team A