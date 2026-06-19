from src.utils.spark_session import create_spark_session


def main() -> None:
    spark = create_spark_session("CheckSpark")

    data = [
        ("P0001", "SITE_A", "ACTIVE"),
        ("P0002", "SITE_B", "COMPLETED"),
        ("P0003", "SITE_C", "WITHDRAWN"),
    ]

    columns = ["patient_id", "site_id", "status"]

    df = spark.createDataFrame(data, columns)

    df.show()
    df.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()