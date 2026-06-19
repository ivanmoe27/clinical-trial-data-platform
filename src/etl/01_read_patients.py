from pathlib import Path

from src.utils.spark_session import create_spark_session


BASE_DIR = Path(__file__).resolve().parents[2]
PATIENTS_PATH = BASE_DIR / "data" / "landing" / "patients.csv"


def main() -> None:
    spark = create_spark_session("ReadPatients")

    patients_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(str(PATIENTS_PATH))
    )

    print("Patients dataset:")
    patients_df.show(10, truncate=False)

    print("Patients schema:")
    patients_df.printSchema()

    print(f"Total rows: {patients_df.count()}")

    spark.stop()


if __name__ == "__main__":
    main()