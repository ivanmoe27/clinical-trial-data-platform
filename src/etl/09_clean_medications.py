from pathlib import Path

from pyspark.sql.functions import col, coalesce, lit, to_date, trim, upper, when

from src.utils.spark_session import create_spark_session


BASE_DIR = Path(__file__).resolve().parents[2]

RAW_MEDICATIONS_PATH = BASE_DIR / "data" / "raw" / "medications"
CLEANED_MEDICATIONS_PATH = BASE_DIR / "data" / "cleaned" / "medications"
DATA_QUALITY_MEDICATIONS_PATH = BASE_DIR / "data" / "data_quality" / "medications_issues"


def main() -> None:
    spark = create_spark_session("CleanMedications")

    medications_df = spark.read.parquet(str(RAW_MEDICATIONS_PATH))

    print("Original medications schema:")
    medications_df.printSchema()

    print("Original medications sample:")
    medications_df.show(10, truncate=False)

    print("Distinct dose_unit values:")
    medications_df.select("dose_unit").distinct().show(truncate=False)

    print("Distinct route values:")
    medications_df.select("route").distinct().show(truncate=False)

    cleaned_medications_df = (
        medications_df
        .withColumn("drug_name", trim(col("drug_name")))
        .withColumn(
            "dose_unit",
            when(upper(trim(col("dose_unit"))) == "MILLIGRAMS", "mg")
            .otherwise(trim(col("dose_unit")))
        )
        .withColumn(
            "administration_date",
            coalesce(
                to_date(col("administration_date"), "yyyy-MM-dd"),
                to_date(col("administration_date"), "yyyy.MM.dd")
            )
        )
        .withColumn("route", upper(trim(col("route"))))
    )

    print("Cleaned medications schema:")
    cleaned_medications_df.printSchema()

    print("Distinct dose_unit values after cleaning:")
    cleaned_medications_df.select("dose_unit").distinct().show(truncate=False)

    print("Administration date sample after cleaning:")
    cleaned_medications_df.select(
        "medication_id",
        "dose",
        "dose_unit",
        "administration_date",
        "route"
    ).show(15, truncate=False)

    negative_dose_issues_df = (
        cleaned_medications_df
        .filter(col("dose") < 0)
        .select(
            lit("medications").alias("dataset_name"),
            col("medication_id").alias("record_id"),
            lit("dose").alias("field_name"),
            lit("INVALID_BUSINESS_VALUE").alias("issue_type"),
            lit("Dose cannot be negative").alias("issue_description"),
            lit("HIGH").alias("severity")
        )
    )    

    print("Negative dose issues:")
    negative_dose_issues_df.show(truncate=False)    

    duplicate_medication_issues_df = (
        cleaned_medications_df
        .groupBy("medication_id")
        .count()
        .filter(col("count") > 1)
        .select(
            lit("medications").alias("dataset_name"),
            col("medication_id").alias("record_id"),
            lit("medication_id").alias("field_name"),
            lit("DUPLICATE_RECORD").alias("issue_type"),
            lit("Duplicate medication_id detected").alias("issue_description"),
            lit("HIGH").alias("severity")
        )
    )

    data_quality_issues_df = negative_dose_issues_df.unionByName(
        duplicate_medication_issues_df
    )

    deduplicated_medications_df = cleaned_medications_df.dropDuplicates(["medication_id"])

    print("Rows before deduplication:")
    print(cleaned_medications_df.count())

    print("Rows after deduplication:")
    print(deduplicated_medications_df.count())

    print("Medications data quality issues summary:")
    data_quality_issues_df.groupBy("issue_type", "severity").count().show(truncate=False)

    (
        deduplicated_medications_df.write
        .mode("overwrite")
        .parquet(str(CLEANED_MEDICATIONS_PATH))
    )

    (
        data_quality_issues_df.write
    .mode("overwrite")
    .parquet(str(DATA_QUALITY_MEDICATIONS_PATH))
    )

    print(f"Cleaned medications written to: {CLEANED_MEDICATIONS_PATH}")
    print(f"Medications data quality issues written to: {DATA_QUALITY_MEDICATIONS_PATH}")

    


    spark.stop()


if __name__ == "__main__":
    main()