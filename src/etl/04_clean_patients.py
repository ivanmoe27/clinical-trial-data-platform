from pathlib import Path

from pyspark.sql.functions import col, upper, trim, when, to_date, coalesce, lit 

from src.utils.spark_session import create_spark_session


BASE_DIR = Path(__file__).resolve().parents[2]

RAW_PATIENTS_PATH = BASE_DIR / "data" / "raw" / "patients"
CLEANED_PATIENTS_PATH = BASE_DIR / "data" / "cleaned" / "patients"
DATA_QUALITY_PATIENTS_PATH = BASE_DIR / "data" / "data_quality" / "patients_issues"



def main() -> None:
    spark = create_spark_session("CleanPatients")

    patients_df = spark.read.parquet(str(RAW_PATIENTS_PATH))

    print("Original patients data:")
    patients_df.select("patient_id", "site_id", "sex").show(10, truncate=False)

    cleaned_patients_df = (
    patients_df
    .withColumn(
        "site_id",
        upper(trim(col("site_id")))
    )
    .withColumn(
        "sex",
        when(upper(trim(col("sex"))) == "FEMALE", "F")
        .when(upper(trim(col("sex"))) == "MALE", "M")
        .otherwise(upper(trim(col("sex"))))
    )
    .withColumn(
        "birth_year",
        col("birth_year").cast("int")
    )
       .withColumn(
        "enrollment_date",
        coalesce(
            to_date(col("enrollment_date"), "yyyy-MM-dd"),
            to_date(col("enrollment_date"), "dd/MM/yyyy")
        )
    ) 
)

    print("Patients data after site_id normalization:")
    cleaned_patients_df.select("patient_id", "site_id", "sex").show(10, truncate=False)
    
    print("Distinct site_id values after normalization:")
    cleaned_patients_df.select("site_id").distinct().show(truncate=False)

    print("Distinct sex values after normalization:")
    cleaned_patients_df.select("sex").distinct().show(truncate=False)

    print("Birth year schema after cleaning:")
    cleaned_patients_df.select("birth_year").printSchema()

    print("Birth year sample after cleaning:")
    cleaned_patients_df.select("patient_id", "birth_year").show(10, truncate=False  )

    print("Enrollment date schema after cleaning:")
    cleaned_patients_df.select("enrollment_date").printSchema()

    print("Enrollment date sample after cleaning:")
    cleaned_patients_df.select("patient_id", "enrollment_date").show(15, truncate=False)



    print("Potential duplicated patients:")

    (
        cleaned_patients_df
        .groupBy("patient_id")
        .count()
        .filter(col("count") > 1)
        .show(truncate=False)
    )

    deduplicated_patients_df = cleaned_patients_df.dropDuplicates(["patient_id"])

    print("Rows before deduplication:")
    print(cleaned_patients_df.count())

    print("Rows after deduplication:")
    print(deduplicated_patients_df.count())

    (
        deduplicated_patients_df.write
        .mode("overwrite")
        .parquet(str(CLEANED_PATIENTS_PATH))
    )

    print(f"Cleaned patients written to: {CLEANED_PATIENTS_PATH}")

    
    duplicate_patient_issues_df = (
    cleaned_patients_df
    .groupBy("patient_id")
    .count()
    .filter(col("count") > 1)
    .select(
        lit("patients").alias("dataset_name"),
        col("patient_id").alias("record_id"),
        lit("patient_id").alias("field_name"),
        lit("DUPLICATE_RECORD").alias("issue_type"),
        lit("Duplicate patient_id detected").alias("issue_description"),
        lit("HIGH").alias("severity")
        )
    )
  
    missing_birth_year_issues_df = (
        cleaned_patients_df
        .filter(col("birth_year").isNull())
        .select(
            lit("patients").alias("dataset_name"),
            col("patient_id").alias("record_id"),
            lit("birth_year").alias("field_name"),
            lit("MISSING_VALUE").alias("issue_type"),
            lit("Missing birth_year value").alias("issue_description"),
            lit("MEDIUM").alias("severity")
        )
    )

    data_quality_issues_df = duplicate_patient_issues_df.unionByName(
    missing_birth_year_issues_df
    )


    (
        data_quality_issues_df.write
        .mode("overwrite")
        .parquet(str(DATA_QUALITY_PATIENTS_PATH))
    )

    print(f"Patient data quality issues written to: {DATA_QUALITY_PATIENTS_PATH}")

    print("Data quality issues summary:")
    data_quality_issues_df.groupBy("issue_type", "severity").count().show(truncate=False)




    spark.stop()


if __name__ == "__main__":
    main()