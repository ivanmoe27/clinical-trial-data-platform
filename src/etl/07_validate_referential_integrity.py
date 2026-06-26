from pathlib import Path

from pyspark.sql import DataFrame

from pyspark.sql.functions import col, lit

from src.utils.spark_session import create_spark_session


BASE_DIR = Path(__file__).resolve().parents[2]

CLEANED_PATIENTS_PATH = BASE_DIR / "data" / "cleaned" / "patients"
CLEANED_VISITS_PATH = BASE_DIR / "data" / "cleaned" / "visits"
CLEANED_LAB_RESULTS_PATH = BASE_DIR / "data" / "cleaned" / "lab_results"
DATA_QUALITY_REFERENTIAL_PATH = BASE_DIR / "data" / "data_quality" / "referential_integrity_issues"
CLEANED_ADVERSE_EVENTS_PATH = BASE_DIR / "data" / "cleaned" / "adverse_events"
CLEANED_MEDICATIONS_PATH = BASE_DIR / "data" / "cleaned" / "medications"


def validate_foreign_key(
    child_df: DataFrame,
    parent_df: DataFrame,
    foreign_key: str,
    record_id_column: str,
    dataset_name: str,
    issue_description: str,
) -> DataFrame:
    orphan_records_df = (
        child_df
        .join(
            parent_df.select(foreign_key),
            on=foreign_key,
            how="left_anti"
        )
    )

    issues_df = (
        orphan_records_df
        .select(
            lit(dataset_name).alias("dataset_name"),
            col(record_id_column).alias("record_id"),
            lit(foreign_key).alias("field_name"),
            lit("INVALID_FOREIGN_KEY").alias("issue_type"),
            lit(issue_description).alias("issue_description"),
            lit("HIGH").alias("severity")
        )
    )

    return issues_df


def main() -> None:
    spark = create_spark_session("ValidateReferentialIntegrity")

    patients_df = spark.read.parquet(str(CLEANED_PATIENTS_PATH))
    visits_df = spark.read.parquet(str(CLEANED_VISITS_PATH))
    lab_results_df = spark.read.parquet(str(CLEANED_LAB_RESULTS_PATH))
    adverse_events_df = spark.read.parquet(str(CLEANED_ADVERSE_EVENTS_PATH))
    medications_df = spark.read.parquet(str(CLEANED_MEDICATIONS_PATH))

    print("Cleaned patients rows:")
    print(patients_df.count())

    print("Cleaned visits rows:")
    print(visits_df.count())

    print("Cleaned lab results rows:")
    print(lab_results_df.count())

    print("Cleaned adverse events rows:")
    print(adverse_events_df.count())

    print("Cleaned medications rows:")
    print(medications_df.count())

    visit_patient_issues_df = validate_foreign_key(
        child_df=visits_df,
        parent_df=patients_df,
        foreign_key="patient_id",
        record_id_column="visit_id",
        dataset_name="visits",
        issue_description="patient_id does not exist in patients dataset",
    )

    lab_results_visit_issues_df = validate_foreign_key(
        child_df=lab_results_df,
        parent_df=visits_df,
        foreign_key="visit_id",
        record_id_column="result_id",
        dataset_name="lab_results",
        issue_description="visit_id does not exist in visits dataset",
    )

    adverse_events_patient_issues_df = validate_foreign_key(
        child_df=adverse_events_df,
        parent_df=patients_df,
        foreign_key="patient_id",
        record_id_column="event_id",
        dataset_name="adverse_events",
        issue_description="patient_id does not exist in patients dataset",
    )

    medications_visit_issues_df = validate_foreign_key(
        child_df=medications_df,
        parent_df=visits_df,
        foreign_key="visit_id",
        record_id_column="medication_id",
        dataset_name="medications",
        issue_description="visit_id does not exist in visits dataset",
    )         
    
    referential_integrity_issues_df = (
        visit_patient_issues_df
        .unionByName(lab_results_visit_issues_df)
        .unionByName(adverse_events_patient_issues_df)
        .unionByName(medications_visit_issues_df)
    )

    print("Referential integrity issues:")
    referential_integrity_issues_df.show(truncate=False)

    print("Referential integrity issues summary:")
    referential_integrity_issues_df.groupBy("dataset_name", "issue_type", "severity").count().show(truncate=False)

    (
    referential_integrity_issues_df.write
    .mode("overwrite")
    .parquet(str(DATA_QUALITY_REFERENTIAL_PATH))
    )

    print(
        f"Referential integrity issues written to: "
        f"{DATA_QUALITY_REFERENTIAL_PATH}"
    )


    spark.stop()


if __name__ == "__main__":
    main()