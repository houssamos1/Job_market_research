import logging
import pg8000
import os
from dotenv import load_dotenv
from datetime import datetime
from typing import Dict, Any, List, Union
# Configuration de base pour le logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
# Charger les variables d'environnement depuis .env
load_dotenv()

DB_USER = os.getenv("POSTGRES_USER", "root")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "123456")
DB_NAME = os.getenv("POSTGRES_DB", "offers")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5430")
def connect():
    return pg8000.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
    )

def close(conn):
    if conn:
        conn.close()

# --- DIM_DATE ---
def parse_dim_date(date_str: str) -> Dict[str, Union[str, int]]:
    """Parse 'YYYY-MM-DD' en dict pour dim_date."""
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    return {
        "full_date": date_str,
        "jour": date_obj.day,
        "mois": date_obj.month,
        "trimestre": (date_obj.month - 1) // 3 + 1,
        "annee": date_obj.year,
        "jour_semaine": date_obj.isoweekday(),
    }

def get_or_create_dim_date(conn: pg8000.Connection, date_str: str) -> int:
    """Crée ou récupère l'id pour dim_date."""
    fields = parse_dim_date(date_str)
    cursor = conn.cursor()
    cursor.execute("SELECT id_date FROM dim_date WHERE full_date = %s", (fields['full_date'],))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(
        "INSERT INTO dim_date(full_date, jour, mois, trimestre, annee, jour_semaine) "
        "VALUES(%s, %s, %s, %s, %s, %s) RETURNING id_date",
        (fields['full_date'], fields['jour'], fields['mois'], fields['trimestre'], fields['annee'], fields['jour_semaine'])
    )
    id_new = cursor.fetchone()[0]
    conn.commit()
    logging.info(f"Ajout dim_date: {date_str}")
    return id_new

# --- Generic dim creation ---
def get_or_create_dim(
    conn: pg8000.Connection,
    table: str,
    column: str,
    value: Any
) -> int:
    """Crée ou récupère l'id pour une table dim_{table}."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT id_{table} FROM dim_{table} WHERE {column} = %s", (value,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(f"INSERT INTO dim_{table}({column}) VALUES(%s) RETURNING id_{table}", (value,))
    id_new = cursor.fetchone()[0]
    conn.commit()
    logging.info(f"Ajout dim_{table}: {value}")
    return id_new

# --- DIM_COMPAGNIE et DIM_SKILL ---
def get_or_create_dim_compagnie(
    conn: pg8000.Connection,
    compagnie: str,
    secteur: str
) -> int:
    cursor = conn.cursor()
    cursor.execute("SELECT id_compagnie FROM dim_compagnie WHERE compagnie = %s", (compagnie,))
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(
        "INSERT INTO dim_compagnie(compagnie, secteur) VALUES(%s, %s) RETURNING id_compagnie",
        (compagnie, secteur)
    )
    id_new = cursor.fetchone()[0]
    conn.commit()
    logging.info(f"Ajout dim_compagnie: {compagnie}")
    return id_new


def get_or_create_dim_skill(
    conn: pg8000.Connection,
    nom: str,
    type_skill: str
) -> int:
    cursor = conn.cursor()
    cursor.execute(
        "SELECT id_skill FROM dim_skill WHERE nom = %s AND type_skill = %s",
        (nom, type_skill)
    )
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute(
        "INSERT INTO dim_skill(nom, type_skill) VALUES(%s, %s) RETURNING id_skill",
        (nom, type_skill)
    )
    id_new = cursor.fetchone()[0]
    conn.commit()
    logging.info(f"Ajout dim_skill: {nom} ({type_skill})")
    return id_new

# --- Offre CRUD ---
def insert_offer(conn: pg8000.Connection, offer: Dict[str, Any]) -> int:
    """Insère une offre et ses dimensions. Retourne id_offer."""
    cursor = conn.cursor()
    # Vérifier exist
    cursor.execute("SELECT id_offer FROM fact_offre WHERE job_url = %s", (offer['job_url'],))
    if cursor.fetchone():
        logging.info(f"Offre existante: {offer['job_url']}")
        return -1
    # Dimensions
    id_date = get_or_create_dim_date(conn, offer['date_publication'])
    id_source = get_or_create_dim(conn, 'source', 'via', offer['via'])
    id_contrat = get_or_create_dim(conn, 'contrat', 'contrat', offer['contrat'])
    id_titre = get_or_create_dim(conn, 'titre', 'titre', offer['titre'])
    id_edu = get_or_create_dim(conn, 'niveau_etudes', 'niveau_etudes', offer['niveau_etudes'])
    id_exp = get_or_create_dim(conn, 'niveau_experience', 'niveau_experience', offer['niveau_experience'])
    id_comp = get_or_create_dim_compagnie(conn, offer['compagnie'], offer['secteur'])
    # Insert fact
    cursor.execute(
        "INSERT INTO fact_offre(job_url, id_date_publication, id_source, id_contrat, id_titre, "
        "id_compagnie, id_niveau_etudes, id_niveau_experience, description, secteur) "
        "VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id_offer",
        (
            offer['job_url'], id_date, id_source, id_contrat, id_titre,
            id_comp, id_edu, id_exp, offer.get('description'), offer.get('secteur')
        )
    )
    id_offer = cursor.fetchone()[0]
    # Skills
    for sk in offer.get('hard_skills', []):
        id_skill = get_or_create_dim_skill(conn, sk, 'hard')
        cursor.execute("INSERT INTO offre_skill(id_offer, id_skill) VALUES(%s,%s) ON CONFLICT DO NOTHING", (id_offer, id_skill))
    for sk in offer.get('soft_skills', []):
        id_skill = get_or_create_dim_skill(conn, sk, 'soft')
        cursor.execute("INSERT INTO offre_skill(id_offer, id_skill) VALUES(%s,%s) ON CONFLICT DO NOTHING", (id_offer, id_skill))
    conn.commit()
    logging.info(f"Offre insérée: {offer['job_url']} (ID {id_offer})")
    return id_offer


def read_offers(conn: pg8000.Connection) -> List[tuple]:
    """Lit toutes les offres."""
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM fact_offre")
    return cursor.fetchall()


def delete_offer(conn: pg8000.Connection, job_url: str) -> bool:
    """Supprime une offre par job_url."""
    cursor = conn.cursor()
    cursor.execute("DELETE FROM offre_skill WHERE id_offer IN (SELECT id_offer FROM fact_offre WHERE job_url = %s)", (job_url,))
    cursor.execute("DELETE FROM fact_offre WHERE job_url = %s", (job_url,))
    conn.commit()
    logging.info(f"Offre supprimée: {job_url}")
    return True


def update_offer(conn: pg8000.Connection, offer: Dict[str, Any]) -> bool:
    """Met à jour une offre existante et ses dimensions."""
    cursor = conn.cursor()
    # Récupérer id
    cursor.execute("SELECT id_offer FROM fact_offre WHERE job_url = %s", (offer['job_url'],))
    row = cursor.fetchone()
    if not row:
        logging.warning(f"Offre non trouvée: {offer['job_url']}")
        return False
    id_offer = row[0]
    # Dimensions
    id_date = get_or_create_dim_date(conn, offer['date_publication'])
    id_source = get_or_create_dim(conn, 'source', 'via', offer['via'])
    id_contrat = get_or_create_dim(conn, 'contrat', 'contrat', offer['contrat'])
    id_titre = get_or_create_dim(conn, 'titre', 'titre', offer['titre'])
    id_edu = get_or_create_dim(conn, 'niveau_etudes', 'niveau_etudes', offer['niveau_etudes'])
    id_exp = get_or_create_dim(conn, 'niveau_experience', 'niveau_experience', offer['niveau_experience'])
    id_comp = get_or_create_dim_compagnie(conn, offer['compagnie'], offer['secteur'])
    # Mise à jour
    cursor.execute(
        """
        UPDATE fact_offre SET
            id_date_publication=%s, id_source=%s, id_contrat=%s, id_titre=%s,
            id_compagnie=%s, id_niveau_etudes=%s, id_niveau_experience=%s,
            description=%s, secteur=%s
        WHERE id_offer=%s
        """,
        (id_date, id_source, id_contrat, id_titre, id_comp,
         id_edu, id_exp, offer.get('description'), offer.get('secteur'), id_offer)
    )
    # (Optionnel: mettre à jour compétences aussi)
    conn.commit()
    logging.info(f"Offre mise à jour: {offer['job_url']}")
    return True