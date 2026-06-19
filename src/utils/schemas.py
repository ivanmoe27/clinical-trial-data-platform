from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)


PATIENTS_SCHEMA = StructType(
    [
        StructField("patient_id", StringType(), True),
        StructField("site_id", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("birth_year", DoubleType(), True),
        StructField("enrollment_date", StringType(), True),
        StructField("treatment_arm", StringType(), True),
        StructField("status", StringType(), True),
    ]
)


VISITS_SCHEMA = StructType(
    [
        StructField("visit_id", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("site_id", StringType(), True),
        StructField("visit_type", StringType(), True),
        StructField("scheduled_date", StringType(), True),
        StructField("visit_date", StringType(), True),
        StructField("completed", StringType(), True),
        StructField("protocol_deviation", StringType(), True),
    ]
)


ADVERSE_EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("site_id", StringType(), True),
        StructField("event_date", StringType(), True),
        StructField("severity", StringType(), True),
        StructField("relationship_to_treatment", StringType(), True),
        StructField("outcome", StringType(), True),
        StructField("serious_event", StringType(), True),
    ]
)


LAB_RESULTS_SCHEMA = StructType(
    [
        StructField("result_id", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("visit_id", StringType(), True),
        StructField("test_name", StringType(), True),
        StructField("test_value", StringType(), True),
        StructField("unit", StringType(), True),
        StructField("reference_range_low", DoubleType(), True),
        StructField("reference_range_high", DoubleType(), True),
        StructField("abnormal_flag", StringType(), True),
    ]
)


MEDICATIONS_SCHEMA = StructType(
    [
        StructField("medication_id", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("visit_id", StringType(), True),
        StructField("drug_name", StringType(), True),
        StructField("dose", IntegerType(), True),
        StructField("dose_unit", StringType(), True),
        StructField("administration_date", StringType(), True),
        StructField("route", StringType(), True),
    ]
)


SCHEMAS = {
    "patients": PATIENTS_SCHEMA,
    "visits": VISITS_SCHEMA,
    "adverse_events": ADVERSE_EVENTS_SCHEMA,
    "lab_results": LAB_RESULTS_SCHEMA,
    "medications": MEDICATIONS_SCHEMA,
}