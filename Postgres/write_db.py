import os
import json
import pg8000
from datetime import datetime
from dotenv import load_dotenv

# 1) Charger .env
load_dotenv()
DB_USER     = os.getenv("POSTGRES_USER", "root")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "123456")
DB_NAME     = os.getenv("POSTGRES_DB", "offers")
DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "5430"))

DB_CONFIG = {
    "user":     DB_USER,
    "password": DB_PASSWORD,
    "database": DB_NAME,
    "host":     DB_HOST,
    "port":     DB_PORT,
}

# 2) Initialise les compteurs
counters = {
    "dim_contract":    0,
    "dim_work_type":   0,
    "dim_location":    0,
    "dim_company":     0,
    "dim_profile":     0,
    "dim_skill":       0,
    "dim_sector":      0,
    "fact_offer":      0,
    "fact_offer_skill":0,
}

def get_or_create_dim(cur, table, col, value):
    if value is None:
        return None
    tbl = f"dim_{table}"
    cur.execute(f"SELECT {table}_id FROM {tbl} WHERE {col} = %s", (value,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        f"INSERT INTO {tbl} ({col}) VALUES (%s) RETURNING {table}_id",
        (value,),
    )
    new_id = cur.fetchone()[0]
    counters[tbl] += 1
    return new_id

def get_or_create_location(cur, city, country):
    if city is None and country is None:
        return None
    cur.execute(
        "SELECT location_id FROM dim_location WHERE city = %s AND country = %s",
        (city, country),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "INSERT INTO dim_location (city, country) VALUES (%s, %s) RETURNING location_id",
        (city, country),
    )
    new_id = cur.fetchone()[0]
    counters["dim_location"] += 1
    return new_id

def get_or_create_skill(cur, skill, skill_type):
    cur.execute(
        "SELECT skill_id FROM dim_skill WHERE skill = %s AND skill_type = %s",
        (skill, skill_type),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "INSERT INTO dim_skill (skill, skill_type) VALUES (%s, %s) RETURNING skill_id",
        (skill, skill_type),
    )
    new_id = cur.fetchone()[0]
    counters["dim_skill"] += 1
    return new_id

def enrich_db(json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        offers = json.load(f)

    conn = pg8000.connect(**DB_CONFIG)
    cur = conn.cursor()

    for o in offers:
        # Dimensions simples
        contract_id  = get_or_create_dim(cur, "contract",  "contract_type", o.get("contrat"))
        work_type_id = get_or_create_dim(cur, "work_type", "work_type",   o.get("type_travail"))
        company_id   = get_or_create_dim(cur, "company",   "company_name", o.get("company_name"))
        profile_id   = get_or_create_dim(cur, "profile",   "profile",      o.get("profile"))
        sector_list  = o.get("sector") or []
        sector_id    = None
        if sector_list:
            sector_id = get_or_create_dim(cur, "sector", "sector", sector_list[0])

        # Location
        loc = o.get("location", {})
        city    = loc.get("city")
        country = loc.get("country") or loc.get("region")
        location_id = get_or_create_location(cur, city, country)

        # Insert fact_offer
        pub_date = None
        if o.get("publication_date"):
            pub_date = datetime.fromisoformat(o["publication_date"]).date()

        cur.execute(
            """
            INSERT INTO fact_offer
              (job_url, title, publication_date, contract_id, work_type_id,
               location_id, company_id, profile_id, education_years, seniority, sector_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING offer_id
            """,
            (
                o.get("job_url"),
                o.get("titre"),
                pub_date,
                contract_id,
                work_type_id,
                location_id,
                company_id,
                profile_id,
                o.get("education_level"),
                o.get("seniority"),
                sector_id,
            ),
        )
        offer_id = cur.fetchone()[0]
        counters["fact_offer"] += 1

        # Hard skills
        for skill in o.get("hard_skills", []):
            sid = get_or_create_skill(cur, skill, "hard")
            cur.execute(
                "INSERT INTO fact_offer_skill (offer_id, skill_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (offer_id, sid),
            )
            if cur.rowcount > 0:
                counters["fact_offer_skill"] += 1
        # Soft skills
        for skill in o.get("soft_skills", []):
            sid = get_or_create_skill(cur, skill, "soft")
            cur.execute(
                "INSERT INTO fact_offer_skill (offer_id, skill_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (offer_id, sid),
            )
            if cur.rowcount > 0:
                counters["fact_offer_skill"] += 1

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    enrich_db(r"C:\Users\houss\Desktop\DXC\Job_market_research\Postgres\input\corsignal1[1].json")
    # 4) Affichage des compteurs
    print("Import terminé avec succès.")
    for table, count in counters.items():
        print(f"{table} : {count} ligne(s) insérée(s)")
