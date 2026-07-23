import os
import boto3

from config import (
    BUCKET_NAME,
    S3_PREFIX
)

from utils.logger import get_logger

logger = get_logger()

s3 = boto3.client("s3")


def upload_csvs(local_root):
    """
    Upload all CSV files to S3 and delete them locally after
    successful upload.
    """

    uploaded = 0
    failed = 0

    for root, _, files in os.walk(local_root):

        for file in sorted(files):

            if not file.lower().endswith(".csv"):
                continue

            local_path = os.path.join(root, file)

            # Preserve directory structure in S3
            relative_path = os.path.relpath(local_path, local_root)

            s3_key = f"{S3_PREFIX}/{relative_path}"

            logger.info(f"Uploading: {local_path}")

            try:
                # Upload to S3
                s3.upload_file(
                    local_path,
                    BUCKET_NAME,
                    s3_key
                )

                # Verify upload
                s3.head_object(
                    Bucket=BUCKET_NAME,
                    Key=s3_key
                )

                logger.info(
                    f"Uploaded successfully -> s3://{BUCKET_NAME}/{s3_key}"
                )

                # Delete local file
                os.remove(local_path)

                logger.info(f"Deleted local file: {local_path}")

                uploaded += 1

            except Exception as e:

                failed += 1

                logger.exception(
                    f"Failed uploading {local_path}: {str(e)}"
                )

    logger.info("=" * 60)
    logger.info(f"Uploaded Files : {uploaded}")
    logger.info(f"Failed Files   : {failed}")
    logger.info("=" * 60)
