import boto3
import time

CRAWLER_NAME = "flight-gold-crawler"

glue = boto3.client("glue")


def start_crawler():
    try:
        glue.start_crawler(Name=CRAWLER_NAME)
        print(f"Started crawler: {CRAWLER_NAME}")

    except glue.exceptions.CrawlerRunningException:
        print("Crawler is already running.")


def wait_for_completion():

    timeout = 1800  # 30 minutes
    elapsed = 0

    while elapsed < timeout:

        response = glue.get_crawler(Name=CRAWLER_NAME)

        state = response["Crawler"]["State"]

        print(f"Crawler State: {state}")

        if state == "READY":
            print("Crawler completed successfully.")
            return

        time.sleep(15)
        elapsed += 15

    raise TimeoutError("Crawler execution timed out.")

def main():

    start_crawler()

    wait_for_completion()


if __name__ == "__main__":
    main()
