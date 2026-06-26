from pathlib import Path

from pyspark.sql.functions import col, coalesce, lit, to_date, trim, upper, when

from src.utils.spark_session import create_spark_session


BASE_DIR = Path(__file__).resolve().parents[2]

RAW_ADVERSE_EVENTS_PATH = BASE_DIR / "data" / "raw" / "adverse_events"
CLEANED_ADVERSE_EVENTS_PATH = BASE_DIR / "data" / "cleaned" / "adverse_events"
DATA_QUALITY_ADVERSE_EVENTS_PATH = BASE_DIR / "data" / "data_quality" / "adverse_events_issues"


def main() -> None:
    spark = create_spark_session("CleanAdverseEvents")

    adverse_events_df = spark.read.parquet(str(RAW_ADVERSE_EVENTS_PATH))

    print("Original adverse events schema:")
    adverse_events_df.printSchema()

    print("Original adverse events sample:")
    adverse_events_df.show(10, truncate=False)

    print("Distinct severity values:")
    adverse_events_df.select("severity").distinct().show(truncate=False)

    print("Distinct serious_event values:")
    adverse_events_df.select("serious_event").distinct().show(truncate=False)

    cleaned_adverse_events_df = (
        adverse_events_df
        .withColumn("site_id", upper(trim(col("site_id"))))
        .withColumn(
            "event_date",
            coalesce(
                to_date(col("event_date"), "yyyy-MM-dd"),
                to_date(col("event_date"), "MM-dd-yyyy")
            )
        )
        .withColumn(
            "severity",
            when(upper(trim(col("severity"))) == "HIGH", "SEVERE")
            .otherwise(upper(trim(col("severity"))))
        )
        .withColumn("relationship_to_treatment", upper(trim(col("relationship_to_treatment"))))
        .withColumn("outcome", upper(trim(col("outcome"))))
        .withColumn(
            "serious_event",
            when(upper(trim(col("serious_event"))) == "TRUE", "Y")
            .when(upper(trim(col("serious_event"))) == "FALSE", "N")
            .otherwise(upper(trim(col("serious_event"))))
        )
    )

    print("Cleaned adverse events schema:")
    cleaned_adverse_events_df.printSchema()

    print("Distinct severity values after cleaning:")
    cleaned_adverse_events_df.select("severity").distinct().show(truncate=False)

    print("Distinct serious_event values after cleaning:")
    cleaned_adverse_events_df.select("serious_event").distinct().show(truncate=False)

    print("Event date sample after cleaning:")
    cleaned_adverse_events_df.select("event_id", "event_date", "severity", "serious_event").show(15, truncate=False)

    duplicate_adverse_event_issues_df = (
        cleaned_adverse_events_df
        .groupBy("event_id")
        .count()
        .filter(col("count") > 1)
        .select(
            lit("adverse_events").alias("dataset_name"),
            col("event_id").alias("record_id"),
            lit("event_id").alias("field_name"),
            lit("DUPLICATE_RECORD").alias("issue_type"),
            lit("Duplicate event_id detected").alias("issue_description"),
            lit("HIGH").alias("severity")
        )
    )

    deduplicated_adverse_events_df = cleaned_adverse_events_df.dropDuplicates(["event_id"])

    print("Rows before deduplication:")
    print(cleaned_adverse_events_df.count())

    print("Rows after deduplication:")
    print(deduplicated_adverse_events_df.count())

    print("Adverse events data quality issues summary:")
    duplicate_adverse_event_issues_df.groupBy("issue_type", "severity").count().show(truncate=False)

    (
        deduplicated_adverse_events_df.write
        .mode("overwrite")
        .parquet(str(CLEANED_ADVERSE_EVENTS_PATH))
    )

    (
        duplicate_adverse_event_issues_df.write
        .mode("overwrite")
        .parquet(str(DATA_QUALITY_ADVERSE_EVENTS_PATH))
    )

    print(f"Cleaned adverse events written to: {CLEANED_ADVERSE_EVENTS_PATH}")
    print(f"Adverse events data quality issues written to: {DATA_QUALITY_ADVERSE_EVENTS_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()