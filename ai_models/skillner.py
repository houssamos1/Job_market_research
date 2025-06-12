import json

import spacy

# load default skills data base
from skillNer.general_params import SKILL_DB

# import skill extractor
from skillNer.skill_extractor_class import SkillExtractor
from spacy.matcher import PhraseMatcher

# init params of skill extractor
nlp = spacy.load("en_core_web_lg")
# init skill extractor
skill_extractor = SkillExtractor(nlp, SKILL_DB, PhraseMatcher)


job_offers = json.load(
    open(
        r"C:\Users\khalm\Documents\Stage2025\Job_analytics\data_extraction\scraping_output\offres_emploi_rekrute.json",
        "r",
        encoding="utf-8",
    )
)

annotations = []

for job_offer in job_offers:
    try:
        annotations.append(skill_extractor.annotate(job_offer["description"]))
    except Exception as e:
        print(f"Exception during the annotation phase: {e}")
        continue

print(annotations)
for i in range(10):
    job_offer = annotations[i]
    print(f"---Job offer n°{i + 1}---")
    for full_match in job_offer["results"]["full_matches"]:
        key = full_match["skill_id"]
        print(
            f"Full match skill {key}---->",
            SKILL_DB[key]["skill_name"],
            "//",
            SKILL_DB[key]["skill_type"],
        )
    for ngram_score in job_offer["results"]["ngram_scored"]:
        key = ngram_score["skill_id"]
        print(
            f"Approximate match skill {key}---->",
            SKILL_DB[key]["skill_name"],
            "//",
            SKILL_DB[key]["skill_type"],
        )
