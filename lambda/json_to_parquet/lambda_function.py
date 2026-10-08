import json
import os
import logging
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
import awswrangler as wr
import pandas as pd


# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ── S3 Configuration ─────────────────────────────────────────────────────────
BRONZE_BUCKET = "yt-data-bronze-europe"
SILVER_BUCKET = "yt-data-silver-europe"

GLUE_DB = os.environ.get(
    "GLUE_DB_SILVER",
    "yt_pipeline_silver_dev"
)

GLUE_TABLE = os.environ.get(
    "GLUE_TABLE_REFERENCE",
    "clean_reference_data"
)

SNS_TOPIC = os.environ.get(
    "SNS_ALERT_TOPIC_ARN",
    ""
)

SILVER_PATH = f"s3://{SILVER_BUCKET}/youtube/reference_data/"


# ── AWS Clients ──────────────────────────────────────────────────────────────
s3_client = boto3.client("s3")
sns_client = boto3.client("sns")


def read_json_from_s3(bucket: str, key: str) -> dict:
    """
    Read raw JSON from S3 using boto3.
    """

    response = s3_client.get_object(
        Bucket=bucket,
        Key=key
    )

    content = response["Body"].read().decode("utf-8")

    return json.loads(content)


def validate_category_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate and clean category reference data.
    """

    if df.empty:
        raise ValueError(
            "Empty DataFrame — no category items found"
        )

    required_cols = {
        "id",
        "snippet.title"
    }

    actual_cols = set(df.columns)

    missing = required_cols - actual_cols

    if missing:
        logger.warning(
            f"Missing expected columns: {missing}. "
            f"Available: {actual_cols}"
        )

    # Remove duplicate categories
    if "id" in df.columns:

        before = len(df)

        df = df.drop_duplicates(
            subset=["id"],
            keep="last"
        )

        after = len(df)

        if before != after:
            logger.info(
                f"Removed {before - after} duplicate categories"
            )

    return df


def send_alert(subject: str, message: str):

    if SNS_TOPIC:

        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Subject=subject[:100],
            Message=message
        )


def lambda_handler(event, context):

    """
    Process JSON files from Bronze S3
    and convert them into Parquet files
    in the Silver S3 bucket.
    """

    records = event.get("Records", [])

    if not records:

        records = [event] if "s3" in event else []

    processed = []
    errors = []

    for record in records:

        key = "unknown"

        try:

            # ── Get S3 information ──────────────────────────────────────

            s3_info = record["s3"]

            bucket = s3_info["bucket"]["name"]

            key = unquote_plus(
                s3_info["object"]["key"]
            )

            logger.info(
                f"Processing: s3://{bucket}/{key}"
            )

            # ── Validate source bucket ──────────────────────────────────

            if bucket != BRONZE_BUCKET:

                raise ValueError(
                    f"Unexpected source bucket: {bucket}. "
                    f"Expected: {BRONZE_BUCKET}"
                )

            # ── Read JSON from Bronze ───────────────────────────────────

            raw_data = read_json_from_s3(
                bucket,
                key
            )

            # ── Convert JSON → DataFrame ────────────────────────────────

            if (
                "items" in raw_data
                and isinstance(raw_data["items"], list)
            ):

                df = pd.json_normalize(
                    raw_data["items"]
                )

            else:

                df = pd.json_normalize(
                    raw_data
                )

            logger.info(
                f"Raw shape: {df.shape}"
            )

            # ── Validate data ───────────────────────────────────────────

            df = validate_category_data(df)

            # ── Add metadata ────────────────────────────────────────────

            df["_ingestion_timestamp"] = (
                datetime.now(timezone.utc).isoformat()
            )

            df["_source_file"] = key

            # ── Extract region ──────────────────────────────────────────

            region = "unknown"

            for part in key.split("/"):

                if part.startswith("region="):

                    region = part.split("=")[1]

                    break

            df["region"] = region

            logger.info(
                f"Clean shape: {df.shape}, "
                f"region: {region}"
            )

            # ── Write Parquet to Silver ─────────────────────────────────

            wr_response = wr.s3.to_parquet(

                df=df,

                path=SILVER_PATH,

                dataset=True,

                database=GLUE_DB,

                table=GLUE_TABLE,

                partition_cols=["region"],

                mode="overwrite_partitions",

                schema_evolution=True,

            )

            logger.info(
                f"Written to Silver: {SILVER_PATH}"
            )

            processed.append({

                "key": key,

                "region": region,

                "rows": len(df)

            })

        except Exception as e:

            logger.error(
                f"Error processing {key}: {e}",
                exc_info=True
            )

            errors.append({

                "key": key,

                "error": str(e)

            })

    # ── Send SNS alert if errors ────────────────────────────────────────

    if errors:

        send_alert(

            subject="[YT Pipeline] Silver reference transform failed",

            message=json.dumps(
                errors,
                indent=2
            )

        )

    # ── Lambda response ─────────────────────────────────────────────────

    return {

        "statusCode": 200,

        "processed": processed,

        "errors": errors

    }