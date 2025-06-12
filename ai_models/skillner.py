import json
import os

import spacy

# load default skills data base
from skillNer.general_params import SKILL_DB

# import skill extractor
from skillNer.skill_extractor_class import SkillExtractor
from spacy.matcher import PhraseMatcher

from database import save_to_minio


def annotate_text(filename="offres_emploi_rekrute.json") -> list:
    """This functions uses spacy's NLP and  skillner's skill extractor and a custom skill database to annotate text.

    This text can displayed using skillextractor's describe and display methods.

    Parameters
    ----------
    filename:
      the name of the json file to extract text from and then annotate
    """
    # init params of skill extractor
    nlp = spacy.load("en_core_web_lg")
    file_path = os.path.join(os.getcwd(), r"data_extraction\scraping_output", filename)
    # init skill extractor
    skill_extractor = SkillExtractor(nlp, SKILL_DB, PhraseMatcher)

    job_offers = json.load(
        open(
            file_path,
            "r",
            encoding="utf-8",
        )
    )

    annotations = []

    for job_offer in job_offers:
        try:
            annotations.append(
                skill_extractor.annotate(
                    job_offer["description"] + job_offer["competences"]
                )
            )

        except Exception as e:
            print(f"Exception during the annotation phase: {e}")
            continue
    return annotations


def extract_skills(filename="offres_emploi_rekrute.json") -> list:
    """Given the filename of a json file, this function will do NER on the skills present in the file's text.

    Parameters
    ---------
    filename:
        The name of the json file
    """
    # First we annotate the text
    annotations = annotate_text(filename=filename)
    ner_data = []
    # During this step we go through the annotations and match the skill id's from SKILL_DB with the skill names
    for i in range(len(annotations)):
        job_offer = annotations[i]
        skills = {}

        print(f"---Job offer n°{i + 1}---")
        for full_match in job_offer["results"]["full_matches"]:
            key = full_match["skill_id"]
            """print(
                f"Full match skill {key}---->",
                SKILL_DB[key]["skill_name"],
                "//",
                SKILL_DB[key]["skill_type"],
            )"""
            skills[SKILL_DB[key]["skill_name"]] = SKILL_DB[key]["skill_type"]
        for ngram_score in job_offer["results"]["ngram_scored"]:
            key = ngram_score["skill_id"]
            """print(
                f"Approximate match skill {key}---->",
                SKILL_DB[key]["skill_name"],
                "//",
                SKILL_DB[key]["skill_type"],
            )"""
            skills[SKILL_DB[key]["skill_name"]] = SKILL_DB[key]["skill_type"]
        ner_data.append(skills)
    ner_filename = "NER_" + filename
    with open(ner_filename, "w", encoding="utf-8") as js_file:
        json.dump(ner_data, js_file, ensure_ascii=False, indent=4)
    save_to_minio(file_path=ner_filename)
    return ner_data


extract_skills()
