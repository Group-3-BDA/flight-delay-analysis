import os
import zipfile

from utils.logger import get_logger

logger = get_logger()


def extract_zip(zip_path, extract_folder="/home/ec2-user/"):
    """
    Extract a ZIP file and return the extracted folder path.
    """

    os.makedirs(extract_folder, exist_ok=True)

    logger.info(f"Extracting: {zip_path}")

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_folder)

    logger.info("Extraction completed.")

    # Assuming ZIP contains FlightData2 folder
    extracted_path = os.path.join(extract_folder, "FlightData2")

    return extracted_path
