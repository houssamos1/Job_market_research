#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import pg8000

# Charger les variables d'environnement
load_dotenv()
DB_CONFIG = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "database": os.getenv("POSTGRES_DB"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
}

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
    return get_or_create(cur, f"public.dim_{table}", {col: value}, f"dim_{table}")


def get_or_create_skill(cur, skill, skill_type):
    if not skill:
        return None
    return get_or_create(
        cur, "public.dim_skill", {"skill": skill, "skill_type": skill_type}, "dim_skill"
    )


def populate_calendar(cur, offers):
    dates = [
        datetime.fromisoformat(o["publication_date"]).date()
        for o in offers
        if o.get("publication_date")
    ]
    if not dates:
        return
    start, end = min(dates), max(dates)
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
            "year_month": d.strftime("%Y-%m"),
            "day_of_week": d.isoweekday(),
            "week_of_year": int(d.strftime("%V")),
            "date_str": d.strftime("%d/%m/%Y"),
        }
        get_or_create(
            cur, "public.dim_calendar", values, "dim_calendar", pk_col="date_id"
        )


def enrich_db(json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        offers = json.load(f)

    # ✅ Export JSON nettoyé
    with open("clean_corsignal.json", "w", encoding="utf-8") as f_out:
        json.dump(offers, f_out, ensure_ascii=False, indent=2)

    print("\n🧼 Le fichier clean_corsignal.json a été exporté avec succès.")

    conn = connect_db()
    cur = conn.cursor()
    populate_calendar(cur, offers)
    conn.commit()

    for o in offers:
        pub_date = (
            datetime.fromisoformat(o["publication_date"]).date()
            if o.get("publication_date")
            else None
        )

        contract_id = get_or_create_dim(
            cur, "contract", "contract_type", o.get("contrat")
        )
        work_type_id = get_or_create_dim(
            cur, "work_type", "work_type", o.get("type_travail")
        )
        company_id = get_or_create_dim(
            cur, "company", "company_name", o.get("company_name")
        )
        profile_id = get_or_create_dim(cur, "profile", "profile", o.get("profile"))
        education_id = get_or_create_dim(
            cur, "education", "education_level", o.get("education_level")
        )
        experience_id = get_or_create_dim(
            cur, "experience", "seniority", o.get("seniority")
        )

        loc = o.get("location", {})
        location_id = get_or_create(
            cur,
            "public.dim_location",
            {
                "city": loc.get("city"),
                "country": loc.get("country") or loc.get("region"),
            },
            "dim_location",
        )

        sector_list = o.get("sector") or []
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
                o.get("source_name") or "unknown",
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
            sid = get_or_create_skill(cur, skill, "hard")
            cur.execute(
                "INSERT INTO public.fact_offer_skill (offer_id, skill_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (offer_id, sid),
            )
            if cur.rowcount > 0:
                counters["fact_offer_skill"] += 1

        for skill in o.get("soft_skills", []):
            sid = get_or_create_skill(cur, skill, "soft")
            cur.execute(
                "INSERT INTO public.fact_offer_skill (offer_id, skill_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (offer_id, sid),
            )
            if cur.rowcount > 0:
                counters["fact_offer_skill"] += 1

    conn.commit()
    cur.close()
    conn.close()

    print("\n✅ Import terminé avec succès.")
    for k, v in counters.items():
        print(f"{k} : {v} ligne(s) insérée(s)")


if __name__ == "__main__":
    enrich_db(r"C:\Users\ouass\Job_market_analytics\corsignal1.json")
