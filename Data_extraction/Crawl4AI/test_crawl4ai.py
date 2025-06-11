import asyncio
import os
import json
from crawl4ai import AsyncWebCrawler
from groq import Groq, RateLimitError, APIError
from jsonschema import validate, ValidationError

# --- Configuration ---
os.environ["GROQ_API_KEY"] = "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"
client = Groq(api_key=os.environ["GROQ_API_KEY"])

# URL cible
URL = "https://www.rekrute.com/offres.html?st=d&keywordNew=1&jobLocation=RK&tagSearchKey=&keyword=data"
URL = URL.strip()

# Schéma JSON pour validation
JOB_SCHEMA = {
    "type": "object",
    "properties": {
        "job_url": {"type": "string"},
        "titre": {"type": "string"},
        "companie": {"type": "string"},
        "description": {"type": "string"},
        "niveau_etudes": {"type": "string"},
        "niveau_experience": {"type": ["string", "null"]},
        "contrat": {"type": "string"},
        "region": {"type": "string"},
        "competences": {"type": "string"},
        "secteur": {"type": "string"},
        "salaire": {"type": ["integer", "string"]},
        "domaine": {"type": "string"},
        "extra": {"type": "string"},
        "via": {"type": "string"},
        "publication_date": {"type": "string", "format": "date"}
    },
    "required": ["job_url", "titre", "via", "publication_date"]
}

#PROMPT = """
#Tu es un extracteur spécialisé en offres d'emploi. 
# À partir des données pré-extraites suivantes, formatte-les en JSON avec la clé 'offres'. 
# Champs : job_url, titre, companie, description, niveau_etudes, niveau_experience, contrat, region, competences, secteur, salaire, domaine, extra, via, publication_date. Utilise 'N/A' si absent, sauf pour job_url, titre, via, publication_date (obligatoires). 
# Format : JSON avec 'offres'.
#"""
#
#

# Prompt pour Groq, adapté au schéma
PROMPT = """
Tu es un extracteur spécialisé en offres d'emploi.

À partir de cette page Rekrute.com, extrait toutes les offres disponibles sous forme de tableau JSON avec la clé "offres". Chaque offre doit contenir les champs suivants :
- job_url (URL de l'offre, obligatoire)
- titre (titre de l'offre, obligatoire)
- companie (nom de l'entreprise)
- description (description de l'offre)
- niveau_etudes (niveau d'études requis)
- niveau_experience (niveau d'expérience requis, peut être null)
- contrat (type de contrat)
- region (région géographique)
- competences (compétences clés)
- secteur (secteur d'activité)
- salaire (salaire, peut être un entier ou une chaîne)
- domaine (domaine de l'offre)
- extra (informations supplémentaires)
- via (source de l'offre, par exemple "Rekrute", obligatoire)
- publication_date (date de publication au format YYYY-MM-DD, obligatoire)

Si une information n'est pas trouvée, utilise "N/A" sauf pour les champs obligatoires (job_url, titre, via, publication_date), qui doivent être présents. Format de sortie : uniquement un tableau JSON valide avec la clé "offres", sans texte supplémentaire.
"""

async def scrape_and_extract_with_groq():
    """Scrape une page de Rekrute et extrait les offres d'emploi avec Crawl4AI et Groq.

    Charge la page cible, extrait le contenu HTML, et utilise Groq pour structurer les offres en JSON.

    Returns:
        str: Chaîne JSON contenant les offres extraites sous la clé "offres", ou None si aucune donnée n'est extraite.
    """
    async with AsyncWebCrawler(verbose=True) as crawler:
        result = await crawler.arun(
            url=URL,
            js_code="""
            const waitForJobs = () => {
                let attempts = 0;
                const maxAttempts = 20;
                return new Promise((resolve) => {
                    const check = () => {
                        const offers = document.querySelectorAll('.offer');
                        if (offers.length > 0) {
                            let offerHtml = '';
                            offers.forEach(offer => {
                                offerHtml += offer.outerHTML + '\n';
                            });
                            document.body.innerHTML = offerHtml; // Remplace par les offres uniquement
                            resolve('Offres trouvées');
                        } else if (attempts < maxAttempts) {
                            attempts++;
                            setTimeout(check, 1000);
                        } else {
                            resolve(false);
                        }
                    };
                    check();
                });
            };
            return waitForJobs();
            """
        )

        if not result.success:
            print("[❌] Échec lors du chargement de la page.")
            return None

        print("[✅] Page chargée avec succès.")
        print("[DEBUG] Longueur du HTML nettoyé :", len(result.cleaned_html))

        # Diviser le HTML en chunks pour respecter la limite de tokens
        chunk_size = 5000  # Environ 1 000 tokens par chunk
        html_chunks = [result.cleaned_html[i:i + chunk_size] for i in range(0, len(result.cleaned_html), chunk_size)]
        print(f"[INFO] Nombre de chunks créés : {len(html_chunks)}")

        # Extraire les données de chaque chunk
        all_offers = []
        for chunk_idx, chunk in enumerate(html_chunks):
            print(f"[INFO] Traitement du chunk {chunk_idx + 1}/{len(html_chunks)} (longueur : {len(chunk)} caractères)")
            try:
                completion = client.chat.completions.create(
                    model="llama-3.3-70b-versatile",
                    response_format={"type": "json_object"},
                    messages=[
                        {"role": "system", "content": "Vous êtes un assistant spécialisé dans l'extraction structurée d'informations."},
                        {"role": "user", "content": f"{PROMPT}\n\nVoici le HTML de la page emploi :\n\n{chunk}"}
                    ]
                )
                chunk_data = json.loads(completion.choices[0].message.content)
                if "offres" in chunk_data:
                    all_offers.extend(chunk_data["offres"])
                else:
                    print(f"[⚠️] Aucun 'offres' trouvé dans le chunk {chunk_idx + 1}")
            except RateLimitError as e:
                print(f"[❌] Limite de tokens atteinte : {e}. Veuillez attendre {e.retry_after} ou passez au Dev Tier (https://console.groq.com/settings/billing). Arrêt du traitement.")
                return None
            except APIError as e:
                print(f"[❌] Erreur API Groq : {e}. Passage au chunk suivant.")
                continue
            except Exception as e:
                print(f"[❌] Erreur inattendue pour le chunk {chunk_idx + 1} : {e}")
                continue

            # Ajouter une pause pour respecter la limite de tokens par minute
            if chunk_idx < len(html_chunks) - 1:
                print("[INFO] Pause de 5 secondes pour éviter la limite de tokens...")
                await asyncio.sleep(5)

        if not all_offers:
            print("[❌] Aucune donnée extraite après traitement de tous les chunks.")
            return None

        # Fusionner les résultats
        extracted_data = json.dumps({"offres": all_offers})
        print("[DEBUG] Données extraites brutes :", extracted_data)
        return extracted_data

async def main():
    """Exécute le processus complet de scraping et d'extraction des offres d'emploi sur Rekrute.

    Coordonne le scraping de la page, l'extraction des données, leur validation, et leur sauvegarde.
    """
    print("[🔍] Démarrage du scraping et extraction IA...\n")
    extracted_content = await scrape_and_extract_with_groq()

    if not extracted_content:
        print("\n❌ Aucune donnée extraite.")
        return

    # Charger les données extraites
    try:
        data = json.loads(extracted_content)
        if "offres" not in data:
            print("\n❌ Les données extraites ne contiennent pas la clé 'offres'.")
            return
        offers = data["offres"]
    except json.JSONDecodeError as e:
        print(f"\n❌ Erreur lors du décodage JSON : {e}")
        return

    # Valider chaque offre par rapport au schéma
    valid_offers = []
    for i, offer in enumerate(offers):
        try:
            validate(instance=offer, schema=JOB_SCHEMA)
            valid_offers.append(offer)
            print(f"[✅] Offre {i + 1} validée avec succès.")
        except ValidationError as e:
            print(f"[⚠️] Offre {i + 1} non valide : {e.message}")

    # Sauvegarder uniquement les offres valides
    if valid_offers:
        final_data = {"offres": valid_offers}
        print("\n🧠 Données validées :")
        print(json.dumps(final_data, indent=2))

        with open("offres_emploi_groq.json", "w", encoding="utf-8") as f:
            json.dump(final_data, f, ensure_ascii=False, indent=2)

        print("\n💾 Données sauvegardées dans 'offres_emploi_groq.json'")
    else:
        print("\n❌ Aucune offre valide à sauvegarder.")

if __name__ == "__main__":
    asyncio.run(main())