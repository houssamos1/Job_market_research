#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import json
from io import BytesIO
from datetime import datetime, timedelta
from minio import Minio
import pg8000

DEFAULT_DATE = datetime(2000, 1, 1).date()

DB_CONFIG = {
    "user": os.getenv("POSTGRES_USER", "root"),
    "password": os.getenv("POSTGRES_PASSWORD", "123456"),
    "database": os.getenv("POSTGRES_DB", "offers"),
    "host": os.getenv("DB_HOST", "postgres"),
    "port": int(os.getenv("DB_PORT", 5432)),
}

MINIO_CLIENT = Minio(
    endpoint=os.getenv("MINIO_API", "minio:9000"),
    access_key=os.getenv("MINIO_ROOT_USER", "TEST"),
    secret_key=os.getenv("MINIO_ROOT_PASSWORD", "12345678"),
    secure=False,
)

counters = {
    "dim_calendar": 0,
    "dim_contract": 0,
    "dim_work_type": 0,
    "dim_location": 0,
    "dim_company": 0,
    "dim_profile": 0,
    "dim_skill": 0,
    "dim_sector": 0,
    "dim_education": 0,
    "dim_experience": 0,
    "fact_offer": 0,
    "fact_offer_skill": 0,
}


def connect_db():
    return pg8000.connect(**DB_CONFIG)


def get_or_create(cur, tbl, col_values: dict, counter_key, pk_col=None):
    if not any(col_values.values()):
        return None
    pk_col = pk_col or f"{tbl.split('.')[-1][4:]}_id"
    where_clause = " AND ".join([f"{col} = %s" for col in col_values])
    values = list(col_values.values())
    cur.execute(f"SELECT {pk_col} FROM {tbl} WHERE {where_clause}", values)
    r = cur.fetchone()
    if r:
        return r[0]
    cols = ", ".join(col_values.keys())
    placeholders = ", ".join(["%s"] * len(col_values))
    cur.execute(
        f"INSERT INTO {tbl} ({cols}) VALUES ({placeholders}) RETURNING {pk_col}", values
    )
    new_id = cur.fetchone()[0]
    counters[counter_key] += 1
    return new_id


def get_or_create_dim(cur, table, col, value):
    if not value or value.strip().lower() == "unknown":
        return None
    return get_or_create(
        cur, f"public.dim_{table}", {col: value.strip().lower()}, f"dim_{table}"
    )


def get_or_create_skill(cur, skill, skill_type):
    if not skill or skill.strip().lower() == "unknown":
        return None
    return get_or_create(
        cur,
        "public.dim_skill",
        {"skill": skill.strip().lower(), "skill_type": skill_type},
        "dim_skill",
    )


def populate_calendar(cur, offers):
    valid_dates = []
    for o in offers:
        d = o.get("publication_date")
        try:
            valid_dates.append(datetime.fromisoformat(d).date())
        except:
            continue
    cur.execute("SELECT 1 FROM public.dim_calendar WHERE date_id = %s", (DEFAULT_DATE,))
    if not cur.fetchone():
        d = DEFAULT_DATE
        values = {
            "date_id": d,
            "year": d.year,
            "quarter": (d.month - 1) // 3 + 1,
            "month_number": d.month,
            "month_name": d.strftime("%B"),
            "day": d.day,
            "year_month": int(d.strftime("%Y%m")),
            "day_of_week": d.isoweekday(),
            "week_of_year": int(d.strftime("%V")),
            "date_str": d.strftime("%d/%m/%Y"),
        }
        get_or_create(
            cur, "public.dim_calendar", values, "dim_calendar", pk_col="date_id"
        )
    if not valid_dates:
        return
    start, end = min(valid_dates), max(valid_dates)
    for i in range((end - start).days + 1):
        d = start + timedelta(days=i)
        cur.execute("SELECT 1 FROM public.dim_calendar WHERE date_id = %s", (d,))
        if cur.fetchone():
            continue
        values = {
            "date_id": d,
            "year": d.year,
            "quarter": (d.month - 1) // 3 + 1,
            "month_number": d.month,
            "month_name": d.strftime("%B"),
            "day": d.day,
            "year_month": int(d.strftime("%Y%m")),
            "day_of_week": d.isoweekday(),
            "week_of_year": int(d.strftime("%V")),
            "date_str": d.strftime("%d/%m/%Y"),
        }
        get_or_create(
            cur, "public.dim_calendar", values, "dim_calendar", pk_col="date_id"
        )


def read_json(obj):
    content = obj.read().decode("utf-8")
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        return json.loads("[" + content.strip().replace("}\n{", "},\n{") + "]")


def insert_data():
    conn = connect_db()
    cur = conn.cursor()

    bucket = "traitement"
    for obj in MINIO_CLIENT.list_objects(bucket, recursive=True):
        print(f"📂 Traitement du fichier : {obj.object_name}")
        data = MINIO_CLIENT.get_object(bucket, obj.object_name)
        offers = read_json(data)
        if not isinstance(offers, list):
            continue

        populate_calendar(cur, offers)
        conn.commit()

        for o in offers:
            try:
                pub_date = datetime.fromisoformat(o.get("publication_date")).date()
            except:
                pub_date = DEFAULT_DATE

            contract_id = get_or_create_dim(
                cur, "contract", "contract_type", o.get("contrat")
            )
            work_type_id = get_or_create_dim(
                cur, "work_type", "work_type", o.get("type_travail")
            )
            company_id = get_or_create_dim(
                cur,
                "company",
                "company_name",
                o.get("company_name") or o.get("companie"),
            )
            profile_id = get_or_create_dim(cur, "profile", "profile", o.get("profile"))
            education_id = get_or_create_dim(
                cur, "education", "education_level", str(o.get("education_level"))
            )
            experience_id = get_or_create_dim(
                cur, "experience", "seniority", o.get("seniority")
            )

            location = o.get("location", {})
            location_id = get_or_create(
                cur,
                "public.dim_location",
                {
                    "city": location.get("city"),
                    "country": location.get("country") or location.get("region"),
                },
                "dim_location",
            )

            sector_list = o.get("sector") or o.get("secteur") or []
            sector_id = (
                get_or_create_dim(cur, "sector", "sector", sector_list[0])
                if sector_list
                else None
            )

            cur.execute(
                """
                INSERT INTO public.fact_offer (
                    source, job_url, title, date_id, contract_id,
                    work_type_id, location_id, company_id, profile_id,
                    education_id, experience_id, sector_id
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING offer_id
                """,
                (
                    o.get("via", "unknown"),
                    o.get("job_url"),
                    o.get("titre"),
                    pub_date,
                    contract_id,
                    work_type_id,
                    location_id,
                    company_id,
                    profile_id,
                    education_id,
                    experience_id,
                    sector_id,
                ),
            )
            offer_id = cur.fetchone()[0]
            counters["fact_offer"] += 1

            for skill in o.get("hard_skills", []):
                if not skill or not skill.strip():
                    continue
                sid = get_or_create_skill(cur, skill, "hard")
                if sid:
                    cur.execute(
                        "INSERT INTO public.fact_offer_skill (offer_id, skill_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (offer_id, sid),
                    )
                    if cur.rowcount > 0:
                        counters["fact_offer_skill"] += 1

            for skill in o.get("soft_skills", []):
                if not skill or not skill.strip():
                    continue
                sid = get_or_create_skill(cur, skill, "soft")
                if sid:
                    cur.execute(
                        "INSERT INTO public.fact_offer_skill (offer_id, skill_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        (offer_id, sid),
                    )
                    if cur.rowcount > 0:
                        counters["fact_offer_skill"] += 1

        conn.commit()
        print(f"✅ Données insérées depuis {obj.object_name}.")

    cur.close()
    conn.close()

    print("\n📊 Statistiques d'insertion :")
    for k, v in counters.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    insert_data()
