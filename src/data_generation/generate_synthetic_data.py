import random
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker


fake = Faker()
random.seed(42)
Faker.seed(42)

BASE_DIR = Path(__file__).resolve().parents[2]
LANDING_DIR = BASE_DIR / "data" / "landing"

NUM_PATIENTS = 100
NUM_VISITS = 250
NUM_ADVERSE_EVENTS = 60
NUM_LAB_RESULTS = 400
NUM_MEDICATIONS = 180

SITES = ["SITE_A", "SITE_B", "SITE_C", "SITE_D"]
TREATMENT_ARMS = ["PLACEBO", "DRUG_A", "DRUG_B"]
SEX_VALUES = ["M", "F"]
PATIENT_STATUSES = ["ACTIVE", "COMPLETED", "WITHDRAWN"]

VISIT_TYPES = ["SCREENING", "BASELINE", "FOLLOW_UP", "FINAL"]
AE_SEVERITIES = ["MILD", "MODERATE", "SEVERE"]
AE_RELATIONSHIP = ["NOT_RELATED", "POSSIBLY_RELATED", "RELATED"]
AE_OUTCOMES = ["RESOLVED", "ONGOING", "RECOVERING"]

LAB_TESTS = {
    "ALT": {"unit": "U/L", "low": 7, "high": 56},
    "AST": {"unit": "U/L", "low": 10, "high": 40},
    "HEMOGLOBIN": {"unit": "g/dL", "low": 12, "high": 17},
    "WBC": {"unit": "10^9/L", "low": 4, "high": 11},
    "CREATININE": {"unit": "mg/dL", "low": 0.6, "high": 1.3},
}

DRUGS = ["PLACEBO", "Investigational Drug A", "Investigational Drug B"]
ROUTES = ["ORAL", "IV", "SC"]


def ensure_directories() -> None:
    LANDING_DIR.mkdir(parents=True, exist_ok=True)


def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days))


def generate_patients() -> pd.DataFrame:
    records = []
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 12, 31)

    for i in range(1, NUM_PATIENTS + 1):
        patient_id = f"P{i:04d}"
        site_id = random.choice(SITES)
        enrollment_date = random_date(start_date, end_date)

        records.append(
            {
                "patient_id": patient_id,
                "site_id": site_id,
                "sex": random.choice(SEX_VALUES),
                "birth_year": random.randint(1945, 2005),
                "enrollment_date": enrollment_date.strftime("%Y-%m-%d"),
                "treatment_arm": random.choice(TREATMENT_ARMS),
                "status": random.choice(PATIENT_STATUSES),
            }
        )

    df = pd.DataFrame(records)

    # Inject realistic data quality issues
    df.loc[5, "sex"] = "Female"
    df.loc[12, "sex"] = "Male"
    df.loc[20, "birth_year"] = None
    df.loc[30, "enrollment_date"] = "15/03/2025"
    df.loc[40, "site_id"] = "site_a"

    # Duplicate record
    df = pd.concat([df, df.iloc[[10]]], ignore_index=True)

    return df


def generate_visits(patients: pd.DataFrame) -> pd.DataFrame:
    records = []
    patient_ids = patients["patient_id"].dropna().unique().tolist()

    for i in range(1, NUM_VISITS + 1):
        patient_id = random.choice(patient_ids)
        site_id = patients.loc[patients["patient_id"] == patient_id, "site_id"].iloc[0]

        scheduled_date = random_date(datetime(2025, 1, 1), datetime(2026, 3, 31))
        visit_date = scheduled_date + timedelta(days=random.randint(-5, 15))

        records.append(
            {
                "visit_id": f"V{i:05d}",
                "patient_id": patient_id,
                "site_id": site_id,
                "visit_type": random.choice(VISIT_TYPES),
                "scheduled_date": scheduled_date.strftime("%Y-%m-%d"),
                "visit_date": visit_date.strftime("%Y-%m-%d"),
                "completed": random.choice(["Y", "N"]),
                "protocol_deviation": random.choice(["Y", "N"]),
            }
        )

    df = pd.DataFrame(records)

    # Inject errors
    df.loc[7, "patient_id"] = "P9999"  # Non-existing patient
    df.loc[15, "visit_date"] = ""  # Missing date
    df.loc[25, "completed"] = "YES"  # Inconsistent boolean format
    df.loc[35, "scheduled_date"] = "2025/05/20"  # Inconsistent date format

    # Duplicate visit
    df = pd.concat([df, df.iloc[[22]]], ignore_index=True)

    return df


def generate_adverse_events(patients: pd.DataFrame) -> pd.DataFrame:
    records = []
    patient_ids = patients["patient_id"].dropna().unique().tolist()

    for i in range(1, NUM_ADVERSE_EVENTS + 1):
        patient_id = random.choice(patient_ids)
        site_id = patients.loc[patients["patient_id"] == patient_id, "site_id"].iloc[0]
        event_date = random_date(datetime(2025, 1, 1), datetime(2026, 3, 31))
        severity = random.choice(AE_SEVERITIES)

        records.append(
            {
                "event_id": f"AE{i:05d}",
                "patient_id": patient_id,
                "site_id": site_id,
                "event_date": event_date.strftime("%Y-%m-%d"),
                "severity": severity,
                "relationship_to_treatment": random.choice(AE_RELATIONSHIP),
                "outcome": random.choice(AE_OUTCOMES),
                "serious_event": "Y" if severity == "SEVERE" and random.random() > 0.4 else "N",
            }
        )

    df = pd.DataFrame(records)

    # Inject errors
    df.loc[4, "severity"] = "high"
    df.loc[9, "patient_id"] = "P8888"
    df.loc[14, "event_date"] = "03-18-2025"
    df.loc[22, "serious_event"] = "TRUE"

    # Duplicate event
    df = pd.concat([df, df.iloc[[11]]], ignore_index=True)

    return df


def generate_lab_results(patients: pd.DataFrame, visits: pd.DataFrame) -> pd.DataFrame:
    records = []
    valid_visits = visits[visits["patient_id"].str.startswith("P", na=False)]

    for i in range(1, NUM_LAB_RESULTS + 1):
        visit = valid_visits.sample(1, random_state=random.randint(1, 100000)).iloc[0]
        test_name = random.choice(list(LAB_TESTS.keys()))
        ref = LAB_TESTS[test_name]

        # Mostly normal values, sometimes abnormal
        if random.random() < 0.85:
            test_value = round(random.uniform(ref["low"], ref["high"]), 2)
        else:
            test_value = round(random.uniform(ref["high"] + 1, ref["high"] * 3), 2)

        abnormal_flag = "Y" if test_value < ref["low"] or test_value > ref["high"] else "N"

        records.append(
            {
                "result_id": f"LR{i:06d}",
                "patient_id": visit["patient_id"],
                "visit_id": visit["visit_id"],
                "test_name": test_name,
                "test_value": test_value,
                "unit": ref["unit"],
                "reference_range_low": ref["low"],
                "reference_range_high": ref["high"],
                "abnormal_flag": abnormal_flag,
            }
        )

    df = pd.DataFrame(records)

    # Inject errors
    df.loc[3, "test_value"] = "not_available"
    df.loc[18, "unit"] = "IU/L"
    df.loc[27, "abnormal_flag"] = "TRUE"
    df.loc[33, "visit_id"] = "V99999"

    # Duplicate result
    df = pd.concat([df, df.iloc[[50]]], ignore_index=True)

    return df


def generate_medications(patients: pd.DataFrame, visits: pd.DataFrame) -> pd.DataFrame:
    records = []
    valid_visits = visits[visits["patient_id"].str.startswith("P", na=False)]

    for i in range(1, NUM_MEDICATIONS + 1):
        visit = valid_visits.sample(1, random_state=random.randint(1, 100000)).iloc[0]

        records.append(
            {
                "medication_id": f"M{i:06d}",
                "patient_id": visit["patient_id"],
                "visit_id": visit["visit_id"],
                "drug_name": random.choice(DRUGS),
                "dose": random.choice([5, 10, 20, 50, 100]),
                "dose_unit": random.choice(["mg", "mg/day"]),
                "administration_date": random_date(
                    datetime(2025, 1, 1), datetime(2026, 3, 31)
                ).strftime("%Y-%m-%d"),
                "route": random.choice(ROUTES),
            }
        )

    df = pd.DataFrame(records)

    # Inject errors
    df.loc[6, "dose"] = -10
    df.loc[16, "dose_unit"] = "milligrams"
    df.loc[26, "administration_date"] = "2025.07.11"
    df.loc[36, "visit_id"] = "V88888"

    # Duplicate medication
    df = pd.concat([df, df.iloc[[44]]], ignore_index=True)

    return df


def save_dataset(df: pd.DataFrame, filename: str) -> None:
    output_path = LANDING_DIR / filename
    df.to_csv(output_path, index=False)
    print(f"Saved {filename}: {len(df)} rows")


def main() -> None:
    ensure_directories()

    patients = generate_patients()
    visits = generate_visits(patients)
    adverse_events = generate_adverse_events(patients)
    lab_results = generate_lab_results(patients, visits)
    medications = generate_medications(patients, visits)

    save_dataset(patients, "patients.csv")
    save_dataset(visits, "visits.csv")
    save_dataset(adverse_events, "adverse_events.csv")
    save_dataset(lab_results, "lab_results.csv")
    save_dataset(medications, "medications.csv")

    print("Synthetic clinical trial datasets generated successfully.")


if __name__ == "__main__":
    main()