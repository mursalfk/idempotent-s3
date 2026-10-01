"""Read the charge counter from DynamoDB after an upload, for the deploy-path demo."""
import os

import boto3

TABLE = os.environ.get("STATE_TABLE", "idempotent-s3-state")
REGION = os.environ.get("AWS_REGION", "eu-west-3")


def main():
    table = boto3.resource("dynamodb", region_name=REGION).Table(TABLE)
    row = table.get_item(Key={"pk": "account#acme"}).get("Item")
    charges = int(row["charges"]) if row and "charges" in row else 0
    verdict = "DOUBLE CHARGED" if charges > 1 else ("no charge" if charges == 0 else "charged once")
    print(f"charge count : {charges}")
    print(f"verdict      : {verdict}")


if __name__ == "__main__":
    main()
