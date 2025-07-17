
import pytest
from datetime import datetime

from db_utils_postgres import (
    connect_db,
    close_db,
    parse_dim_date,
    get_or_create_dim_date,
    get_or_create_dim,
    get_or_create_dim_compagnie,
    get_or_create_dim_skill,
    insert_offer,
    read_offers,
    update_offer,
    delete_offer
)

@pytest.fixture(scope="module")
def conn():
    connection = connect_db()
    yield connection
    close_db(connection)

def test_parse_dim_date():
    parts = parse_dim_date("2025-07-15")
    assert parts["jour"] == 15
    assert parts["mois"] == 7
    assert parts["annee"] == 2025
    assert parts["trimestre"] == 3
    assert parts["jour_semaine"] == datetime(2025, 7, 15).isoweekday()

def test_get_or_create_dim_date(conn):
    # Ensure clean state
    conn.cursor().execute("DELETE FROM dim_date WHERE full_date = %s", ("2025-07-15",))
    conn.commit()
    id1 = get_or_create_dim_date(conn, "2025-07-15")
    id2 = get_or_create_dim_date(conn, "2025-07-15")
    assert id1 == id2

def test_generic_dim(conn):
    conn.cursor().execute("DELETE FROM dim_source WHERE via = %s", ("pytest-source",))
    conn.commit()
    id1 = get_or_create_dim(conn, "source", "via", "pytest-source")
    id2 = get_or_create_dim(conn, "source", "via", "pytest-source")
    assert id1 == id2

def test_dim_compagnie_and_skill(conn):
    cur = conn.cursor()
    cur.execute("DELETE FROM dim_compagnie WHERE compagnie = %s", ("pytest-co",))
    cur.execute("DELETE FROM dim_skill WHERE nom = %s", ("pytest-skill",))
    conn.commit()
    comp_id = get_or_create_dim_compagnie(conn, "pytest-co", "pytest-sector")
    assert isinstance(comp_id, int)
    skill_id = get_or_create_dim_skill(conn, "pytest-skill", "hard")
    assert isinstance(skill_id, int)

def test_crud_offer(conn):
    job_url = "https://test/job/pytest"
    # Clean up
    delete_offer(conn, job_url)
    
    offer = {
        "job_url": job_url,
        "date_publication": "2025-07-15",
        "via": "pytest-source",
        "contrat": "pytest-contract",
        "titre": "pytest-title",
        "compagnie": "pytest-co",
        "secteur": "pytest-sector",
        "niveau_etudes": "pytest-edu",
        "niveau_experience": "pytest-exp",
        "description": "pytest description",
        "hard_skills": ["sk1", "sk2"],
        "soft_skills": ["ss1"]
    }
    # Insert
    new_id = insert_offer(conn, offer)
    assert new_id > 0
    # Read
    rows = read_offers(conn)
    assert any(r[0] == new_id for r in rows)
    # Update
    offer["description"] = "modified"
    updated = update_offer(conn, offer)
    assert updated
    # Delete
    deleted = delete_offer(conn, job_url)
    assert deleted
    # Verify deletion
    cur = conn.cursor()
    cur.execute("SELECT id_offer FROM fact_offre WHERE job_url = %s", (job_url,))
    assert cur.fetchone() is None