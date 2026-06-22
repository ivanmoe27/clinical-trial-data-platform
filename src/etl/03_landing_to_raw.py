from pathlib import Path

from src.utils.spark_session import create_spark_session
from src.utils.schemas import SCHEMAS


BASE_DIR = Path(__file__).resolve().parents[2]

LANDING_DIR = BASE_DIR / "data" / "landing"
RAW_DIR = BASE_DIR / "data" / "raw"


DATASETS = {
    "patients": LANDING_DIR / "patients.csv",
    "visits": LANDING_DIR / "visits.csv",
    "adverse_events": LANDING_DIR / "adverse_events.csv",
    "lab_results": LANDING_DIR / "lab_results.csv",
    "medications": LANDING_DIR / "medications.csv",
}


def read_dataset(spark, dataset_name, path):
    return (
        spark.read
        .option("header", "true")
        .schema(SCHEMAS[dataset_name])
        .csv(str(path))
    )


def write_raw_parquet(df, dataset_name):
    output_path = RAW_DIR / dataset_name

    (
        df.write
        .mode("overwrite")
        .parquet(str(output_path))
    )

    print(f"Written RAW dataset: {output_path}")


def main():

    spark = create_spark_session("LandingToRaw")

    for dataset_name, dataset_path in DATASETS.items():

        print("=" * 80)
        print(f"Processing {dataset_name}")

        df = read_dataset(
            spark=spark,
            dataset_name=dataset_name,
            path=dataset_path
        )

        print(f"Rows read: {df.count()}")

        write_raw_parquet(
            df=df,
            dataset_name=dataset_name
        )

    spark.stop()


if __name__ == "__main__":
    main()