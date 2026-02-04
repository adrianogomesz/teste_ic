# Imports
from pathlib import Path

# Imports de módulos do projeto
from api_ans.scraper import find_dir, find_last_three_quarters
from api_ans.downloader import download_and_extract, download_file
from api_ans.transformer import run_transformation


BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/"
OPERADORAS_URL = (
    "https://dadosabertos.ans.gov.br/FTP/PDA/"
    "operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
)

RAW_DIR = Path("data/raw")


def main():
    print("Iniciando o processo de ETL da ANS.")

    # =========================
    # ETAPA 1 — SCRAPING E DOWNLOAD DOS TRIMESTRES
    # =========================
    demo_url = find_dir(BASE_URL)
    last_zips = find_last_three_quarters(demo_url)

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    extracted_dirs = []

    for url in last_zips:
        print("Processando:", url)
        extract_dir = download_and_extract(url, RAW_DIR)
        extracted_dirs.append(extract_dir)

    print("Download e extração dos trimestres concluídos.")

    # =========================
    # ETAPA 2 — COLETA DOS CSVs TRIMESTRAIS
    # =========================
    quarterly_csvs = []
    for dir_path in extracted_dirs:
        quarterly_csvs.extend(dir_path.glob("*.csv"))

    if not quarterly_csvs:
        raise RuntimeError("Nenhum CSV trimestral encontrado após extração.")

    # =========================
    # ETAPA 3 — DOWNLOAD DO CADOP
    # =========================
    cadop_csv = RAW_DIR / "Relatorio_cadop.csv"

    if not cadop_csv.exists():
        print("Baixando cadastro de operadoras (CADOP)...")
        download_file(OPERADORAS_URL, cadop_csv)
    else:
        print("CADOP já existe, pulando download.")

    # =========================
    # ETAPA 4 — TRANSFORMAÇÃO COMPLETA
    # =========================
    run_transformation(
        quarterly_csvs=quarterly_csvs,
        cadop_csv=cadop_csv
    )


if __name__ == "__main__":
    main()
