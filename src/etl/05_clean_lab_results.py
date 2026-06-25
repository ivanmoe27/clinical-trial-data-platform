from pathlib import Path

from pyspark.sql.functions import col, lit, trim, upper, when

from src.utils.spark_session import create_spark_session


BASE_DIR = Path(__file__).resolve().parents[2]

RAW_LAB_RESULTS_PATH = BASE_DIR / "data" / "raw" / "lab_results"
CLEANED_LAB_RESULTS_PATH = BASE_DIR / "data" / "cleaned" / "lab_results"
DATA_QUALITY_LAB_RESULTS_PATH = BASE_DIR / "data" / "data_quality" / "lab_results_issues"


def main() -> None:
    spark = create_spark_session("CleanLabResults")

    lab_results_df = spark.read.parquet(str(RAW_LAB_RESULTS_PATH))

    cleaned_lab_results_df = (
        lab_results_df
        .withColumn(
            "test_name",
            upper(trim(col("test_name")))
        )
        .withColumn(
            "unit",
            trim(col("unit"))
        )
        .withColumn(
            "test_value_numeric",
            col("test_value").cast("double")
        )
        .withColumn(
            "abnormal_flag",
            when(upper(trim(col("abnormal_flag"))) == "TRUE", "Y")
            .when(upper(trim(col("abnormal_flag"))) == "FALSE", "N")
            .otherwise(upper(trim(col("abnormal_flag"))))    
        )
    )


    invalid_test_value_issues_df = (
        cleaned_lab_results_df
        .filter(
            col("test_value").isNotNull()
            & col("test_value_numeric").isNull()
        )
        .select(
            lit("lab_results").alias("dataset_name"),
            col("result_id").alias("record_id"),
            lit("test_value").alias("field_name"),
            lit("INVALID_NUMERIC_VALUE").alias("issue_type"),
            lit("test_value could not be converted to double").alias("issue_description"),
            lit("HIGH").alias("severity")
        )
    )


    print("Original lab results schema:")
    lab_results_df.printSchema()

    print("Original lab results sample:")
    lab_results_df.show(10, truncate=False)

    print("Distinct abnormal_flag values:")
    lab_results_df.select("abnormal_flag").distinct().show(truncate=False)

    print("Lab results after numeric conversion:")
    cleaned_lab_results_df.select(
        "result_id",
        "test_name",
        "test_value",
        "test_value_numeric",
        "unit"
    ).show(10, truncate=False)

    print("Invalid test_value issues:")
    invalid_test_value_issues_df.show(truncate=False)

    print("Distinct abnormal_flag values after cleaning")
    cleaned_lab_results_df.select("abnormal_flag").distinct().show(truncate=False)
    
    (
        cleaned_lab_results_df.write
        .mode("overwrite")
        .parquet(str(CLEANED_LAB_RESULTS_PATH))                 
    )

    (
        invalid_test_value_issues_df.write
        .mode("overwrite")
        .parquet(str(DATA_QUALITY_LAB_RESULTS_PATH))
    )

    print(f"Cleaned lab results written  to: {DATA_QUALITY_LAB_RESULTS_PATH}")
    print(f"Lab results data quality issues written to:{DATA_QUALITY_LAB_RESULTS_PATH}")

    spark.stop()


if __name__ == "__main__":
    main()