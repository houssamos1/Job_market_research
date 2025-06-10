#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json

def build_mcd(offers):
    # Dictionnaires de dimensions
    D = {
        "dim_contract":   {},
        "dim_work_type":  {},
        "dim_location":   {},
        "dim_company":    {},
        "dim_profile":    {},
        "dim_skill":      {},
        "dim_sector":     {}
    }
    # Counters
    C = {k: 1 for k in D}

    fact_offer       = []
    fact_offer_skill = []

    for idx, o in enumerate(offers, start=1):
        # Contrat
        ct = (o.get("contrat") or "unknown").lower()
        if ct not in D["dim_contract"]:
            D["dim_contract"][ct] = C["dim_contract"]
            C["dim_contract"] += 1
        cid = D["dim_contract"][ct]

        # Work type
        wt = (o.get("type_travail") or "unknown").lower()
        if wt not in D["dim_work_type"]:
            D["dim_work_type"][wt] = C["dim_work_type"]
            C["dim_work_type"] += 1
        wtid = D["dim_work_type"][wt]

        # Location
        loc = o.get("location", {})
        city    = (loc.get("city") or "unknown").lower()
        country = (loc.get("country") or "unknown").lower()
        lk      = f"{city}|{country}"
        if lk not in D["dim_location"]:
            D["dim_location"][lk] = C["dim_location"]
            C["dim_location"] += 1
        lid = D["dim_location"][lk]

        # Company
        comp = (o.get("company_name") or "unknown").lower()
        if comp not in D["dim_company"]:
            D["dim_company"][comp] = C["dim_company"]
            C["dim_company"] += 1
        compid = D["dim_company"][comp]

        # Profile
        prof = (o.get("profile") or "unknown").lower()
        if prof not in D["dim_profile"]:
            D["dim_profile"][prof] = C["dim_profile"]
            C["dim_profile"] += 1
        profid = D["dim_profile"][prof]

        # Sector
        secs = o.get("sector") or ["unknown"]
        sec = secs[0].lower()
        if sec not in D["dim_sector"]:
            D["dim_sector"][sec] = C["dim_sector"]
            C["dim_sector"] += 1
        secid = D["dim_sector"][sec]

        # Offer row
        fact_offer.append({
            "offer_id":         idx,
            "job_url":          o.get("job_url"),
            "title":            o.get("titre"),
            "publication_date": o.get("publication_date"),
            "contract_id":      cid,
            "work_type_id":     wtid,
            "location_id":      lid,
            "company_id":       compid,
            "profile_id":       profid,
            "education_years":  o.get("experience_years"),
            "seniority":        o.get("seniority"),
            "sector_id":        secid
        })

        # Skills (hard + soft)
        for typ in ("hard", "soft"):
            for sk in o.get(f"{typ}_skills", []):
                key = (sk.lower(), typ)
                if key not in D["dim_skill"]:
                    D["dim_skill"][key] = C["dim_skill"]
                    C["dim_skill"] += 1
                sid = D["dim_skill"][key]
                fact_offer_skill.append({
                    "offer_id": idx,
                    "skill_id": sid
                })

    # Convertir dimensions en listes
    mcd = {}
    for table, mapping in D.items():
        rows = []
        for k, vid in mapping.items():
            if table == "dim_location":
                city, country = k.split("|",1)
                rows.append({"location_id": vid, "city": city, "country": country})
            elif table == "dim_skill":
                skill, typ = k
                rows.append({"skill_id": vid, "skill": skill, "skill_type": typ})
            else:
                col = table.split("_",1)[1]
                rows.append({f"{col}_id": vid, col: k})
        mcd[table] = rows

    mcd["fact_offer"]       = fact_offer
    mcd["fact_offer_skill"] = fact_offer_skill
    return mcd

def main():
    with open(r"C:\Users\houss\Desktop\DXC\Job_market_research\Postgres\input\corsignal1[1].json", encoding="utf-8") as f:
        offers = json.load(f)
    mcd = build_mcd(offers)
    with open("mcd_final.json", "w", encoding="utf-8") as f:
        json.dump(mcd, f, indent=2, ensure_ascii=False)
    print("✅ mcd_final.json généré")

if __name__ == "__main__":
    main()
