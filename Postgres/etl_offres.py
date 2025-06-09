#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json

def build_mcd(offers):
    # dimensions
    profiles, locations, salaries = {}, {}, {}
    contrats, work_types, remotes = {}, {}, {}
    companies, sectors = {}, {}
    vias, seniorities, educ_levels = {}, {}, {}
    soft_skills, hard_skills = {}, {}

    # tables de fait
    offers_table = []
    offer_contracts = []
    offer_work_types = []
    offer_remotes = []
    offer_companies = []
    offer_sectors = []
    offer_vias = []
    offer_seniorities = []
    offer_educations = []
    offer_soft_skills = []
    offer_hard_skills = []

    # compteurs
    counters = {
        'profile': 1, 'location': 1, 'salary': 1,
        'contract': 1, 'work_type': 1, 'remote': 1,
        'company': 1, 'sector': 1, 'via': 1,
        'seniority': 1, 'education': 1,
        'soft_skill': 1, 'hard_skill': 1
    }

    for idx, o in enumerate(offers, start=1):
        if not o.get("is_data_profile"):
            continue
        oid = idx

        # --- dim_profile ---
        prof = str(o.get("profile") or "unknown").strip().lower()
        if prof not in profiles:
            profiles[prof] = counters['profile']; counters['profile'] += 1
        pid = profiles[prof]

        # --- dim_location ---
        loc = o.get("location", {})
        if isinstance(loc, dict):
            city = loc.get("city") or ""
            region = loc.get("region") or ""
            country = loc.get("country") or ""
            loc_str = f"{city},{region},{country}".strip(", ").lower()
        else:
            loc_str = str(loc or "unknown").strip().lower()
        if loc_str not in locations:
            locations[loc_str] = counters['location']; counters['location'] += 1
        lid = locations[loc_str]

        # --- dim_salaire ---
        sal = o.get("salary_range") or {}
        if isinstance(sal, dict):
            lo = sal.get("min") or ""
            hi = sal.get("max") or ""
            cur = sal.get("currency") or ""
            per = sal.get("period") or ""
            sal_str = f"{lo}-{hi} {cur}/{per}".strip()
        else:
            sal_str = str(sal).strip()
        if sal_str not in salaries:
            salaries[sal_str] = counters['salary']; counters['salary'] += 1
        sid = salaries[sal_str]

        # --- dim_contrat ---
        cont = str(o.get("contrat") or "unknown").strip().lower()
        if cont not in contrats:
            contrats[cont] = counters['contract']; counters['contract'] += 1
        cid = contrats[cont]
        offer_contracts.append({"offer_id": oid, "contract_id": cid})

        # --- dim_work_type ---
        wt = str(o.get("type_travail") or "unknown").strip().lower()
        if wt not in work_types:
            work_types[wt] = counters['work_type']; counters['work_type'] += 1
        wtid = work_types[wt]
        offer_work_types.append({"offer_id": oid, "work_type_id": wtid})

        # --- dim_remote ---
        is_r = bool(o.get("location", {}).get("remote", False))
        if is_r not in remotes:
            remotes[is_r] = counters['remote']; counters['remote'] += 1
        rid = remotes[is_r]
        offer_remotes.append({"offer_id": oid, "remote_id": rid})

        # --- dim_company ---
        comp = str(o.get("company_name") or "unknown").strip().lower()
        if comp not in companies:
            companies[comp] = counters['company']; counters['company'] += 1
        compid = companies[comp]
        offer_companies.append({"offer_id": oid, "company_id": compid})

        # --- dim_sector (multi) ---
        for s in o.get("sector") or []:
            sec = str(s).strip().lower()
            if sec not in sectors:
                sectors[sec] = counters['sector']; counters['sector'] += 1
            secid = sectors[sec]
            offer_sectors.append({"offer_id": oid, "sector_id": secid})

        # --- dim_via ---
        v = str(o.get("via") or "unknown").strip().lower()
        if v not in vias:
            vias[v] = counters['via']; counters['via'] += 1
        vid = vias[v]
        offer_vias.append({"offer_id": oid, "via_id": vid})

        # --- dim_seniority ---
        sr = str(o.get("seniority") or "unknown").strip().lower()
        if sr not in seniorities:
            seniorities[sr] = counters['seniority']; counters['seniority'] += 1
        srid = seniorities[sr]
        offer_seniorities.append({"offer_id": oid, "seniority_id": srid})

        # --- dim_education_level ---
        el = str(o.get("education_level") or "unknown").strip().lower()
        if el not in educ_levels:
            educ_levels[el] = counters['education']; counters['education'] += 1
        elid = educ_levels[el]
        offer_educations.append({"offer_id": oid, "education_id": elid})

        # --- offre principale ---
        offers_table.append({
            "offer_id": oid,
            "job_url": o.get("job_url",""),
            "titre": o.get("titre",""),
            "description": o.get("description",""),
            "publication_date": o.get("publication_date",""),
            "profile_id": pid,
            "location_id": lid,
            "salary_id": sid
        })

        # --- soft_skills & hard_skills ---
        for sk in o.get("soft_skills") or []:
            key = str(sk).strip().lower()
            if key not in soft_skills:
                soft_skills[key] = counters['soft_skill']; counters['soft_skill'] += 1
            skill_id = soft_skills[key]
            offer_soft_skills.append({"offer_id": oid, "skill_id": skill_id})

        for sk in o.get("hard_skills") or []:
            key = str(sk).strip().lower()
            if key not in hard_skills:
                hard_skills[key] = counters['hard_skill']; counters['hard_skill'] += 1
            skill_id = hard_skills[key]
            offer_hard_skills.append({"offer_id": oid, "skill_id": skill_id})

    # Assemblage final
    return {
        "offers": offers_table,
        "profiles":    [{"profile_id": v, "profile": k} for k, v in profiles.items()],
        "locations":   [{"location_id": v, "location": k} for k, v in locations.items()],
        "salaries":    [{"salary_id": v, "salary_range": k} for k, v in salaries.items()],
        "contrats":    [{"contract_id": v, "contract_type": k} for k, v in contrats.items()],
        "work_types":  [{"work_type_id": v, "work_type": k} for k, v in work_types.items()],
        "remotes":     [{"remote_id": v, "is_remote": k} for k, v in remotes.items()],
        "companies":   [{"company_id": v, "company_name": k} for k, v in companies.items()],
        "sectors":     [{"sector_id": v, "sector": k} for k, v in sectors.items()],
        "vias":        [{"via_id": v, "via": k} for k, v in vias.items()],
        "seniorities":[{"seniority_id":v,"seniority":k} for k,v in seniorities.items()],
        "educations":  [{"education_id":v,"education_level":k} for k,v in educ_levels.items()],
        "soft_skills": [{"skill_id": v, "skill": k, "type": "soft"} for k, v in soft_skills.items()],
        "hard_skills": [{"skill_id": v, "skill": k, "type": "hard"} for k, v in hard_skills.items()],
        "offer_contracts":   offer_contracts,
        "offer_work_types":  offer_work_types,
        "offer_remotes":     offer_remotes,
        "offer_companies":   offer_companies,
        "offer_sectors":     offer_sectors,
        "offer_vias":        offer_vias,
        "offer_seniorities": offer_seniorities,
        "offer_educations":  offer_educations,
        "offer_soft_skills": offer_soft_skills,
        "offer_hard_skills": offer_hard_skills
    }


def main():
    input_file = r"C:\Users\houss\Desktop\DXC\Job_market_research\Postgres\input\corsignal1[1].json"
    output_file = r"C:\Users\houss\Desktop\DXC\Job_market_research\mcd_output_full.json"

    if not os.path.exists(input_file):
        print(f"❌ Le fichier n'existe pas : {input_file}")
        return

    with open(input_file, encoding="utf-8") as f:
        offers = json.load(f)

    result = build_mcd(offers)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"✅ JSON complet généré : {output_file}")


if __name__ == "__main__":
    main()
