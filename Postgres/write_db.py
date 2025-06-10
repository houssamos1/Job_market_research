#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json
from dotenv import load_dotenv
import pg8000.dbapi

# --- Chargement des variables d’environnement ---
load_dotenv()
conn = pg8000.dbapi.connect(
    user     = os.getenv("POSTGRES_USER", "root"),
    password = os.getenv("POSTGRES_PASSWORD", "root"),
    database = os.getenv("POSTGRES_DB", "offers"),
    host     = os.getenv("DB_HOST", "localhost"),
    port     = int(os.getenv("DB_PORT", 5430))
)
cur = conn.cursor()

def insert_ignore(table, row, conflict_keys):
    cols = list(row.keys())
    vals = [row[c] for c in cols]
    ph   = ", ".join(["%s"]*len(vals))
    cc   = ", ".join(conflict_keys)
    sql  = f"""
        INSERT INTO public.{table} ({', '.join(cols)})
        VALUES ({ph})
        ON CONFLICT ({cc}) DO NOTHING;
    """
    try:
        cur.execute(sql, vals)
    except Exception as e:
        conn.rollback()
        print(f"❌ Erreur table public.{table}: {e}\nLigne: {row}")

# --- Chargement du JSON transformé ---
with open(r"C:\Users\houss\Desktop\DXC\Job_market_research\mcd_final.json", encoding="utf-8") as f:
    data = json.load(f)

# --- 1) Insert dimensions dans public.dim_* ---
dim_mapping = {
    "dim_contract":       ["contract_id"],
    "dim_work_type":      ["work_type_id"],
    "dim_location":       ["location_id"],
    "dim_company":        ["company_id"],
    "dim_profile":        ["profile_id"],
    "dim_skill":          ["skill_id"],
    "dim_sector":         ["sector_id"]
}

for table, pkeys in dim_mapping.items():
    for row in data.get(table, []):
        insert_ignore(table, row, pkeys)

# --- 2) Insert dans public.fact_offer ---
for r in data.get("fact_offer", []):
    row = {
        "offer_id":         r["offer_id"],
        "job_url":          r.get("job_url"),
        "title":            r.get("titre"),
        "publication_date": r.get("publication_date"),
        "contract_id":      r.get("contract_id"),
        "work_type_id":     r.get("work_type_id"),
        "location_id":      r.get("location_id"),
        "company_id":       r.get("company_id"),
        "profile_id":       r.get("profile_id"),
        "education_years":  r.get("education_years"),
        "seniority":        r.get("seniority"),
        "sector_id":        r.get("sector_id")
    }
    insert_ignore("fact_offer", row, ["offer_id"])

# --- 3) Insert dans public.fact_offer_skill ---
for r in data.get("fact_offer_skill", []):
    insert_ignore("fact_offer_skill", r, ["offer_id","skill_id"])

# --- Commit & fermeture ---
conn.commit()
cur.close()
conn.close()
print("✅ Chargement terminé dans schema public.")  
