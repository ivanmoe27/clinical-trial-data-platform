from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "ClinicalTrialDataPlatform") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )