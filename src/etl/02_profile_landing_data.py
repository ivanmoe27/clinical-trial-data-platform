from pathlib import Path

from src.utils.spark_session import create_spark_session
from src.utils.schemas import SCHEMAS


BASE_DIR = Path(__file__).resolve().parents[2]
LANDING_DIR = BASE_DIR / "data" / "landing"

DATASETS = {
    "patients": LANDING_DIR / "patients.csv",
    "visits": LANDING_DIR / "visits.csv",
    "adverse_events": LANDING_DIR / "adverse_events.csv",
    "lab_results": LANDING_DIR / "lab_results.csv",
    "medications": LANDING_DIR / "medications.csv",
}


def read_csv_with_schema(spark, dataset_name: str, path: Path):
    return (
        spark.read
        .option("header", "true")
        .schema(SCHEMAS[dataset_name])
        .csv(str(path))
    )


def main() -> None:
    spark = create_spark_session("ProfileLandingDataWithSchemas")

    for dataset_name, dataset_path in DATASETS.items():
        print("=" * 80)
        print(f"Dataset: {dataset_name}")
        print(f"Path: {dataset_path}")

        df = read_csv_with_schema(spark, dataset_name, dataset_path)

        print(f"Rows: {df.count()}")
        print(f"Columns: {df.columns}")

        print("Schema:")
        df.printSchema()

        print("Sample:")
        df.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()