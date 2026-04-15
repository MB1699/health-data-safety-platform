from __future__ import annotations

import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from app.config import RAW_DIR, ensure_directories


FIRST_NAMES = [
    "Mia",
    "Noah",
    "Ava",
    "Ethan",
    "Liam",
    "Olivia",
    "Zoe",
    "Aria",
    "Lucas",
    "Emma",
    "Elijah",
    "Sofia",
    "James",
    "Harper",
    "Amelia",
]

LAST_NAMES = [
    "Carter",
    "Lopez",
    "Nguyen",
    "Patel",
    "Davis",
    "Garcia",
    "Wilson",
    "Reed",
    "Kim",
    "Brown",
    "Martinez",
    "Young",
    "Adams",
    "Bennett",
    "Price",
]

CITIES = [
    ("Boston", "MA"),
    ("Dallas", "TX"),
    ("Tampa", "FL"),
    ("Phoenix", "AZ"),
    ("Denver", "CO"),
    ("Columbus", "OH"),
    ("Seattle", "WA"),
    ("Atlanta", "GA"),
]

SOURCE_SYSTEMS = ["ehr", "payer_roster", "lab_feed", "hl7", "fax_referral"]
FACILITIES = ["North Medical Center", "Lakeside Clinic", "Metro Health", "Harbor Hospital"]
PAYERS = ["Aetna", "United", "Cigna", "Blue Shield", "Humana"]
CLAIM_STATUS = ["paid", "pending", "denied"]
LAB_TYPES = ["A1C", "CBC", "Lipid Panel", "Creatinine", "TSH"]
DIAGNOSIS_CODES = ["E11.9", "I10", "J45.909", "M54.5", "Z00.00", "R73.03"]
GENDER_VARIANTS = {
    "female": ["Female", "F", "f", "woman"],
    "male": ["Male", "M", "m", "man"],
    "unknown": ["Unknown", "U", "", "not listed"],
}


def _random_date(rng: random.Random, start_year: int, end_year: int) -> date:
    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    return start + timedelta(days=rng.randint(0, (end - start).days))


def _random_phone(rng: random.Random) -> str:
    digits = "".join(str(rng.randint(0, 9)) for _ in range(10))
    styles = [
        digits,
        f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}",
        f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}",
    ]
    return rng.choice(styles)


def _random_zip(rng: random.Random) -> str:
    base = f"{rng.randint(10000, 99999)}"
    return rng.choice([base, f"{base}-{rng.randint(1000, 9999)}", f" {base} "])


def _format_dob(dob_value: date, rng: random.Random) -> str:
    options = [
        dob_value.strftime("%Y-%m-%d"),
        dob_value.strftime("%m/%d/%Y"),
        dob_value.strftime("%d-%b-%Y"),
        dob_value.strftime("%Y/%m/%d"),
    ]
    return rng.choice(options)


def _mutate_name(name: str, rng: random.Random) -> str:
    options = [name, name.lower(), name.upper(), f" {name} "]
    if len(name) > 3:
        options.append(name[:-1])
    return rng.choice(options)


def _email(first_name: str, last_name: str, rng: random.Random, patient_number: int) -> str:
    domain = rng.choice(["examplehealth.org", "maildemo.net", "patientmail.com"])
    return f"{first_name.lower()}.{last_name.lower()}{patient_number}@{domain}"


def _address(rng: random.Random, patient_number: int) -> str:
    street_number = 100 + (patient_number * 7) % 800
    street_name = rng.choice(["Maple", "Oak", "River", "Hill", "Cedar", "Park"])
    suffix = rng.choice(["St", "Ave", "Blvd", "Ln"])
    return f"{street_number} {street_name} {suffix}"


def _canonical_patient(patient_number: int, rng: random.Random) -> dict[str, str]:
    first_name = rng.choice(FIRST_NAMES)
    last_name = rng.choice(LAST_NAMES)
    city, state = rng.choice(CITIES)
    gender_key = rng.choice(list(GENDER_VARIANTS))
    dob_value = _random_date(rng, 1948, 2005)
    return {
        "patient_number": patient_number,
        "first_name": first_name,
        "last_name": last_name,
        "dob": dob_value,
        "phone": _random_phone(rng),
        "email": _email(first_name, last_name, rng, patient_number),
        "address": _address(rng, patient_number),
        "city": city,
        "state": state,
        "zip_code": _random_zip(rng),
        "gender_key": gender_key,
        "mrn": f"MRN{patient_number:05d}",
    }


def _variant_row(canonical: dict[str, str], variant_number: int, rng: random.Random) -> dict[str, str]:
    source_system = rng.choice(SOURCE_SYSTEMS)
    email_value = canonical["email"] if rng.random() > 0.2 else ""
    mrn_value = canonical["mrn"] if rng.random() > 0.28 else ""
    updated_at = datetime(2026, 4, 1, 8, 0, 0) + timedelta(hours=rng.randint(0, 240))
    return {
        "source_system": source_system,
        "source_record_id": f"{source_system.upper()}-{canonical['patient_number']:04d}-{variant_number}",
        "first_name": _mutate_name(canonical["first_name"], rng),
        "last_name": _mutate_name(canonical["last_name"], rng),
        "dob": _format_dob(canonical["dob"], rng),
        "phone": canonical["phone"] if rng.random() > 0.1 else _random_phone(rng),
        "email": email_value,
        "address": canonical["address"] if rng.random() > 0.08 else f"{canonical['address']} Apt {rng.randint(2, 20)}",
        "city": canonical["city"],
        "state": canonical["state"].lower() if rng.random() > 0.7 else canonical["state"],
        "zip_code": canonical["zip_code"],
        "gender": rng.choice(GENDER_VARIANTS[canonical["gender_key"]]),
        "mrn": mrn_value,
        "updated_at": updated_at.isoformat(timespec="seconds"),
    }


def _build_patients(seed: int, patient_count: int) -> list[dict[str, str]]:
    rng = random.Random(seed)
    rows: list[dict[str, str]] = []
    for patient_number in range(1, patient_count + 1):
        canonical = _canonical_patient(patient_number, rng)
        variant_count = 1
        if patient_number % 6 == 0:
            variant_count = 3
        elif patient_number % 4 == 0:
            variant_count = 2

        for variant_number in range(1, variant_count + 1):
            rows.append(_variant_row(canonical, variant_number, rng))
    return rows


def _build_encounters(seed: int, patients: list[dict[str, str]]) -> list[dict[str, str]]:
    rng = random.Random(seed + 11)
    rows: list[dict[str, str]] = []
    encounter_number = 1
    for patient in patients:
        for _ in range(rng.randint(1, 2)):
            admit_at = datetime(2025, 1, 1) + timedelta(days=rng.randint(0, 365), hours=rng.randint(0, 23))
            discharge_at = admit_at + timedelta(hours=rng.randint(2, 60))
            rows.append(
                {
                    "encounter_id": f"ENC-{encounter_number:06d}",
                    "source_patient_id": patient["source_record_id"],
                    "facility_name": rng.choice(FACILITIES),
                    "admit_at": admit_at.isoformat(timespec="seconds"),
                    "discharge_at": discharge_at.isoformat(timespec="seconds"),
                    "diagnosis_code": rng.choice(DIAGNOSIS_CODES),
                    "provider_npi": f"{rng.randint(1000000000, 9999999999)}",
                }
            )
            encounter_number += 1
    return rows


def _build_claims(seed: int, patients: list[dict[str, str]]) -> list[dict[str, str]]:
    rng = random.Random(seed + 29)
    rows: list[dict[str, str]] = []
    claim_number = 1
    for patient in patients:
        for _ in range(rng.randint(1, 3)):
            billed = round(rng.uniform(120.0, 4200.0), 2)
            paid = round(billed * rng.uniform(0.2, 0.98), 2)
            rows.append(
                {
                    "claim_id": f"CLM-{claim_number:06d}",
                    "source_patient_id": patient["source_record_id"],
                    "payer_name": rng.choice(PAYERS),
                    "claim_status": rng.choices(CLAIM_STATUS, weights=[0.72, 0.18, 0.1])[0],
                    "billed_amount": billed,
                    "paid_amount": paid,
                    "claim_date": (_random_date(rng, 2025, 2026)).isoformat(),
                }
            )
            claim_number += 1
    return rows


def _build_labs(seed: int, patients: list[dict[str, str]]) -> list[dict[str, str]]:
    rng = random.Random(seed + 47)
    rows: list[dict[str, str]] = []
    lab_number = 1
    for patient in patients:
        if rng.random() < 0.85:
            rows.append(
                {
                    "lab_id": f"LAB-{lab_number:06d}",
                    "source_patient_id": patient["source_record_id"],
                    "lab_name": rng.choice(LAB_TYPES),
                    "result_value": round(rng.uniform(0.7, 14.5), 2),
                    "result_unit": rng.choice(["mg/dL", "g/dL", "%", "mmol/L"]),
                    "abnormal_flag": rng.choice(["Y", "N"]),
                    "collected_at": (
                        datetime(2025, 1, 1) + timedelta(days=rng.randint(0, 365), hours=rng.randint(0, 23))
                    ).isoformat(timespec="seconds"),
                }
            )
            lab_number += 1
    return rows


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def generate_raw_data(seed: int = 17, patient_count: int = 120) -> dict[str, int]:
    ensure_directories()
    patients = _build_patients(seed=seed, patient_count=patient_count)
    encounters = _build_encounters(seed=seed, patients=patients)
    claims = _build_claims(seed=seed, patients=patients)
    labs = _build_labs(seed=seed, patients=patients)

    _write_csv(RAW_DIR / "patients_raw.csv", patients)
    _write_csv(RAW_DIR / "encounters_raw.csv", encounters)
    _write_csv(RAW_DIR / "claims_raw.csv", claims)
    _write_csv(RAW_DIR / "labs_raw.csv", labs)

    return {
        "patients_raw": len(patients),
        "encounters_raw": len(encounters),
        "claims_raw": len(claims),
        "labs_raw": len(labs),
    }


if __name__ == "__main__":
    print(generate_raw_data())
