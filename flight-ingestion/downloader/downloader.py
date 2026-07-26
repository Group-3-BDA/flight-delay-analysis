import os
import subprocess

from config import (
    KAGGLE_DATASET,
    DOWNLOAD_FOLDER
)

from utils.logger import get_logger
from utils.helpers import create_directory

logger = get_logger()


def download_dataset():

    create_directory(DOWNLOAD_FOLDER)

    logger.info("Downloading dataset from Kaggle...")

    command = [

        "kaggle",

        "datasets",

        "download",

        "-d",

        KAGGLE_DATASET,

        "-p",

        DOWNLOAD_FOLDER

    ]

    result = subprocess.run(
        command,
        capture_output=True,
        text=True
    )

    if result.returncode != 0:

        logger.error(result.stderr)

        raise Exception("Dataset download failed.")

    logger.info(result.stdout)

    for file in os.listdir(DOWNLOAD_FOLDER):

        if file.endswith(".zip"):

            zip_path = os.path.join(DOWNLOAD_FOLDER, file)

            logger.info(f"ZIP Downloaded : {zip_path}")

            return zip_path

    raise FileNotFoundError("ZIP file not found.")
