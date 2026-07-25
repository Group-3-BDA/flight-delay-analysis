from downloader.kaggle_download import download_dataset
from uploader.upload_s3 import upload_zip
from extractor.extract import extract_zip
from uploader.upload_s3 import upload_csvs

def main():

    print("=" * 60)
    print("Flight Data Ingestion Started")
    print("=" * 60)

    zip_path = download_dataset()

    upload_zip(zip_path)

    extracted_folder = extract_zip(zip_path)

    upload_csvs(extracted_folder)

    print("Pipeline Completed Successfully")

if __name__ == "__main__":
    main()
