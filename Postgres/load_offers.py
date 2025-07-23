# import os
# import logging
# from _init_postgres import load_offers_from_file

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )

# def main():
#     # Chemin vers le fichier JSON d'offres
#     script_dir = os.path.dirname(os.path.abspath(__file__))
#     input_file = os.path.join(script_dir, "offres_data_ai_2025.json")

#     if not os.path.exists(input_file):
#         logging.error(f"❌ Fichier introuvable : {input_file}")
#         return

#     logging.info(f"🚀 Démarrage du chargement des offres depuis {input_file}...")
    
#     try:
#         load_offers_from_file(input_file)
#         logging.info("✅ Chargement des offres terminé avec succès.")
#     except Exception as e:
#         logging.error(f"❌ Erreur lors du chargement : {e}")

# if __name__ == "__main__":
#     main()

from _init_postgres import load_offers_from_minio

if __name__ == "__main__":
    load_offers_from_minio("traitement")
