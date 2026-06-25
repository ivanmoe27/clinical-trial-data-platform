from pathlib import Path

from pyspark.sql.functions import col, coalesce, lit, to_date, trim, upper, when

from src.utils.spark_session import create_spark_session


BASE_DIR = Path(__file__).resolve().parents[2]

RAW_VISITS_PATH = BASE_DIR / "data" / "raw" / "visits"
CLEANED_VISITS_PATH = BASE_DIR / "data" / "cleaned" / "visits"
DATA_QUALITY_VISITS_PATH = BASE_DIR / "data" / "data_quality" / "visits_issues"


def main() -> None:
    spark = create_spark_session("CleanVisits")

    visits_df = spark.read.parquet(str(RAW_VISITS_PATH))

    print("Original visits schema:")
    visits_df.printSchema()

    print("Original visits sample:")
    visits_df.show(10, truncate=False)

    cleaned_visits_df = (
        visits_df
        .withColumn("site_id", upper(trim(col("site_id"))))
        .withColumn("visit_type", upper(trim(col("visit_type"))))
        .withColumn(
            "scheduled_date",
            coalesce(
                to_date(col("scheduled_date"), "yyyy-MM-dd"),
                to_date(col("scheduled_date"), "yyyy/MM/dd")
            )
        )
        .withColumn(
            "visit_date",
            coalesce(
                to_date(col("visit_date"), "yyyy-MM-dd"),
                to_date(col("visit_date"), "yyyy/MM/dd")
            )
        )
        .withColumn(
            "completed",
            when(upper(trim(col("completed"))) == "YES", "Y")
            .when(upper(trim(col("completed"))) == "TRUE", "Y")
            .when(upper(trim(col("completed"))) == "NO", "N")
            .when(upper(trim(col("completed"))) == "FALSE", "N")
            .otherwise(upper(trim(col("completed"))))
        )
        .withColumn(
            "protocol_deviation",
            when(upper(trim(col("protocol_deviation"))) == "YES", "Y")
            .when(upper(trim(col("protocol_deviation"))) == "TRUE", "Y")
            .when(upper(trim(col("protocol_deviation"))) == "NO", "N")
            .when(upper(trim(col("protocol_deviation"))) == "FALSE", "N")
            .otherwise(upper(trim(col("protocol_deviation"))))
        )
    )

    print("Cleaned visits schema:")
    cleaned_visits_df.printSchema()

    print("Distinct completed values after cleaning:")
    cleaned_visits_df.select("completed").distinct().show(truncate=False)

    print("Distinct protocol_deviation values after cleaning:")
    cleaned_visits_df.select("protocol_deviation").distinct().show(truncate=False)

    missing_visit_date_issues_df = (
        cleaned_visits_df
        .filter(col("visit_date").isNull())
        .select(
            lit("visits").alias("dataset_name"),
            col("visit_id").alias("record_id"),
            lit("visit_date").alias("field_name"),
            lit("MISSING_VALUE").alias("issue_type"),
            lit("Missing visit_date value").alias("issue_description"),
            lit("MEDIUM").alias("severity")
        )
    )

    duplicate_visit_issues_df = (
        cleaned_visits_df
        .groupBy("visit_id")
        .count()
        .filter(col("count") > 1)
        .select(
            lit("visits").alias("dataset_name"),
            col("visit_id").alias("record_id"),
            lit("visit_id").alias("field_name"),
            lit("DUPLICATE_RECORD").alias("issue_type"),
            lit("Duplicate visit_id detected").alias("issue_description"),
            lit("HIGH").alias("severity")
        )
    )

    data_quality_issues_df = missing_visit_date_issues_df.unionByName(
        duplicate_visit_issues_df
    )

    deduplicated_visits_df = cleaned_visits_df.dropDuplicates(["visit_id"])

    print("Rows before deduplication:")
    print(cleaned_visits_df.count())

    print("Rows after deduplication:")
    print(deduplicated_visits_df.count())

    print("Visits data quality issues summary:")
    data_quality_issues_df.groupBy("issue_type", "severity").count().show(truncate=False)

    (
        deduplicated_visits_df.write
        .mode("overwrite")
        .parquet(str(CLEANED_VISITS_PATH))
    )

    (
        data_quality_issues_df.write
        .mode("overwrite")
        .parquet(str(DATA_QUALITY_VISITS_PATH))
    )

    print(f"Cleaned visits written to: {CLEANED_VISITS_PATH}")
    print(f"Visits data quality issues written to: {DATA_QUALITY_VISITS_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()