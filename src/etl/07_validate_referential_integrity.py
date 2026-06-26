from pathlib import Path

from pyspark.sql.functions import col, lit

from src.utils.spark_session import create_spark_session


BASE_DIR = Path(__file__).resolve().parents[2]

CLEANED_PATIENTS_PATH = BASE_DIR / "data" / "cleaned" / "patients"
CLEANED_VISITS_PATH = BASE_DIR / "data" / "cleaned" / "visits"
CLEANED_LAB_RESULTS_PATH = BASE_DIR / "data" / "cleaned" / "lab_results"
DATA_QUALITY_REFERENTIAL_PATH = BASE_DIR / "data" / "data_quality" / "referential_integrity_issues"

def main() -> None:
    spark = create_spark_session("ValidateReferentialIntegrity")

    patients_df = spark.read.parquet(str(CLEANED_PATIENTS_PATH))
    visits_df = spark.read.parquet(str(CLEANED_VISITS_PATH))
    lab_results_df = spark.read.parquet(str(CLEANED_LAB_RESULTS_PATH))

    print("Cleaned patients rows:")
    print(patients_df.count())

    print("Cleaned visits rows:")
    print(visits_df.count())

    print("Cleaned lab results rows:")
    print(lab_results_df.count())

    orphan_visits_df = (
        visits_df
        .join(
            patients_df.select("patient_id"),
            on="patient_id",
            how="left_anti"
        )
    )

    print("Visits with non-existing patient_id")
    orphan_visits_df.select("visit_id", "patient_id", "site_id", "visit_type").show(truncate=False)

    print("Number of visits with non-existing patient_id:")
    print(orphan_visits_df.count())

    
    print("Orphan visit issues:")
    orphan_visits_df.show(truncate=False)
    
    orphan_lab_results_df = (
        lab_results_df
        .join(
            visits_df.select("visit_id"),
            on="visit_id",
            how="left_anti"
        )
    )

    print("Lab results with non-existing visit_id:")
    orphan_lab_results_df.select("result_id", "visit_id", "patient_id", "test_name").show(truncate=False)

    print("Number of lab results with non-existing visit_id")
    print(orphan_lab_results_df.count())

    orphan_visit_issues_df = (
        orphan_visits_df
        .select(
            lit("visits").alias("dataset_name"),
            col("visit_id").alias("record_id"),
            lit("patient_id").alias("field_name"),
            lit("INVALID_FOREIGN_KEY").alias("issue_type"),
            lit("patient_id does not exist in patients dataset").alias("issue_description"),
            lit("HIGH").alias("severity")
        )
    )

    orphan_lab_result_issues_df = (
        orphan_lab_results_df
        .select(
            lit("lab_results").alias("dataset_name"),
            col("result_id").alias("record_id"),
            lit("visit_id").alias("field_name"),
            lit("INVALID_FOREIGN_KEY").alias("issue_type"),
            lit("visit_id does not exist in visits dataset").alias("issue_description"),
            lit("HIGH").alias("severity")
        )
    )

    referential_integrity_issues_df = orphan_visit_issues_df.unionByName(
        orphan_lab_result_issues_df
    )

    print("Referential integrity issues:")
    referential_integrity_issues_df.show(truncate=False)

    print("Referential integrity issues summary:")
    referential_integrity_issues_df.groupBy("dataset_name", "issue_type", "severity").count().show(truncate=False)


    spark.stop()


if __name__ == "__main__":
    main()