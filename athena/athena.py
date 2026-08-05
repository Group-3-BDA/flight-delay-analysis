import os
import time
import boto3

DATABASE = os.environ.get("ATHENA_DATABASE", "flight_gold")
WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")

GOLD_BUCKET = os.environ["GOLD_BUCKET"]

OUTPUT_LOCATION = f"s3://{GOLD_BUCKET}/athena-results/"
SQL_BUCKET = GOLD_BUCKET
SQL_PREFIX = "athena/sql/"

athena = boto3.client("athena")
s3 = boto3.client("s3")


def execute_query(query):

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            "Database": DATABASE
        },
        WorkGroup=WORKGROUP,
        ResultConfiguration={
            "OutputLocation": OUTPUT_LOCATION
        }
    )

    execution_id = response["QueryExecutionId"]

    while True:

        status = athena.get_query_execution(
            QueryExecutionId=execution_id
        )

        state = status["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            print("Query executed successfully.")
            return

        if state in ["FAILED", "CANCELLED"]:

            reason = status["QueryExecution"]["Status"].get(
                "StateChangeReason",
                "Unknown Error"
            )

            raise Exception(reason)

        time.sleep(5)


def run_sql_files():

    objects = s3.list_objects_v2(
        Bucket=SQL_BUCKET,
        Prefix=SQL_PREFIX
    )

    for obj in objects.get("Contents", []):

        key = obj["Key"]

        if not key.endswith(".sql"):
            continue

        print(f"Executing {key}")

        sql = (
            s3.get_object(
                Bucket=SQL_BUCKET,
                Key=key
            )["Body"]
            .read()
            .decode("utf-8")
        )

        execute_query(sql)


def main():

    run_sql_files()


if __name__ == "__main__":
    main()
