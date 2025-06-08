#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os

def build_mcd(offers):
    offers_table = []
    profiles_table = {}
    locations_table = {}
    salaries_table = {}
    contrats_table = {}
    work_types_table = {}
    remote_table = {}
    companies_table = {}
    sectors_table = {}
    soft_skill_ids = {}
    hard_skill_ids = {}

    offer_contracts = []
    offer_work_types = []
    offer_remotes = []
    offer_companies = []
    offer_sectors = []
    offer_soft_skills = []
    offer_hard_skills = []

    # compteurs
    pid = lid = sid = cid = wtid = rid = compid = secid = 1
    skill_idx = 1

    for i, o in enumerate(offers):
        if o.get("is_data_profile") != 1:
            continue
        oid = i + 1

        # profile
        prof = str(o.get("profile","unknown")).strip().lower()
        if prof not in profiles_table:
            profiles_table[prof] = pid; pid += 1
        profile_id = profiles_table[prof]

        # location
        loc = o.get("location",{})
        if isinstance(loc,dict):
            loc_txt = ", ".join(filter(None, [
                loc.get("ville",""), loc.get("region",""), loc.get("pays","")
            ])).strip().lower()
        else:
            loc_txt = str(loc or "unknown").strip().lower()
        if loc_txt not in locations_table:
            locations_table[loc_txt] = lid; lid += 1
        location_id = locations_table[loc_txt]

        # salary
        sal = o.get("salary_range") or {}
        if isinstance(sal,dict):
            sal_txt = f"{sal.get('min','')}-{sal.get('max','')} {sal.get('devise','')}".strip()
        else:
            sal_txt = str(sal).strip()
        if sal_txt not in salaries_table:
            salaries_table[sal_txt] = sid; sid += 1
        salary_id = salaries_table[sal_txt]

        # contrat
        cont = str(o.get("contrat","unknown")).strip().lower()
        if cont not in contrats_table:
            contrats_table[cont] = cid; cid += 1
        contract_id = contrats_table[cont]
        offer_contracts.append({"offer_id":oid, "contract_id":contract_id})

        # work_type
        wt = str(o.get("type_travail","unknown")).strip().lower()
        if wt not in work_types_table:
            work_types_table[wt] = wtid; wtid += 1
        work_type_id = work_types_table[wt]
        offer_work_types.append({"offer_id":oid, "work_type_id":work_type_id})

        # remote
        is_r = bool(o.get("remote",False))
        if is_r not in remote_table:
            remote_table[is_r] = rid; rid += 1
        remote_id = remote_table[is_r]
        offer_remotes.append({"offer_id":oid, "remote_id":remote_id})

        # company
        comp = str(o.get("company_name","unknown")).strip().lower()
        if comp not in companies_table:
            companies_table[comp] = compid; compid += 1
        company_id = companies_table[comp]
        offer_companies.append({"offer_id":oid, "company_id":company_id})

        # sectors (array)
        for s in o.get("sector") or []:
            sec = str(s).strip().lower()
            if sec not in sectors_table:
                sectors_table[sec] = secid; secid += 1
            sid2 = sectors_table[sec]
            offer_sectors.append({"offer_id":oid, "sector_id":sid2})

        # main offer row
        offers_table.append({
            "offer_id":  oid,
            "job_url":   o.get("job_url",""),
            "titre":     o.get("titre",""),
            "via":       o.get("via",""),
            "description": o.get("description",""),
            "publication_date": o.get("publication_date",""),
            "education_level":  o.get("education_level",None),
            "experience_years": o.get("experience_years",None),
            "seniority":        o.get("seniority",""),
            "profile_id":       profile_id,
            "location_id":      location_id,
            "salary_id":        salary_id
        })

        # skills
        for sk in o.get("soft_skills") or []:
            key = sk.strip().lower()
            if key not in soft_skill_ids:
                soft_skill_ids[key] = skill_idx; skill_idx += 1
            offer_soft_skills.append({"offer_id":oid, "skill_id":soft_skill_ids[key]})
        for sk in o.get("hard_skills") or []:
            key = sk.strip().lower()
            if key not in hard_skill_ids:
                hard_skill_ids[key] = skill_idx; skill_idx += 1
            offer_hard_skills.append({"offer_id":oid, "skill_id":hard_skill_ids[key]})

    return {
        "offers": offers_table,
        "profiles":    [{"profile_id":v,"profile":k} for k,v in profiles_table.items()],
        "locations":   [{"location_id":v,"location":k} for k,v in locations_table.items()],
        "salaries":    [{"salary_id":v,"salary_range":k} for k,v in salaries_table.items()],
        "contrats":    [{"contract_id":v,"contract_type":k} for k,v in contrats_table.items()],
        "work_types":  [{"work_type_id":v,"work_type":k} for k,v in work_types_table.items()],
        "remotes":     [{"remote_id":v,"is_remote":k} for k,v in remote_table.items()],
        "companies":   [{"company_id":v,"company_name":k} for k,v in companies_table.items()],
        "sectors":     [{"sector_id":v,"sector":k} for k,v in sectors_table.items()],
        "soft_skills": [{"skill_id":v,"skill":k} for k,v in soft_skill_ids.items()],
        "hard_skills": [{"skill_id":v,"skill":k} for k,v in hard_skill_ids.items()],
        "offer_contracts":   offer_contracts,
        "offer_work_types":  offer_work_types,
        "offer_remotes":     offer_remotes,
        "offer_companies":   offer_companies,
        "offer_sectors":     offer_sectors,
        "offer_soft_skills": offer_soft_skills,
        "offer_hard_skills": offer_hard_skills
    }

def main():
    input_file  = r"C:\Users\houss\Desktop\DXC\Job_market_research\Postgres\input\corsignal1[1].json"
    output_file = r"C:\Users\houss\Desktop\DXC\Job_market_research\mcd_output_extended.json"

    if not os.path.exists(input_file):
        print(f"❌ Le fichier n'existe pas : {input_file}")
        return

    with open(input_file, encoding="utf-8") as f:
        offers = json.load(f)

    result = build_mcd(offers)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"✅ JSON étendu généré : {output_file}")

if __name__ == "__main__":
    main()
