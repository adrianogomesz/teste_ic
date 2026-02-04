import pandas as pd
import os
import zipfile


from validate_docbr import CNPJ
from api_ans.downloader import download_file
from api_ans.utils import format_brl
from pathlib import Path


# =========================
# CONFIGURAÇÕES
# =========================

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# LEITURA E LIMPEZA DOS CSVs TRIMESTRAIS
# =========================

def read_and_clean_quarter_csv(
    csv_path: Path,
    ano: int,
    trimestre: int
) -> pd.DataFrame:
    """
    Lê um CSV trimestral da ANS e retorna apenas os dados
    necessários para o pipeline:

    - REG_ANS (chave de join)
    - ValorDespesas
    - Ano
    - Trimestre
    """

    df = pd.read_csv(
        csv_path,
        sep=";",
        encoding="latin1"
    )

    # Normaliza colunas (CSV governamental)
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
    )

    # Mapeamento para o schema interno
    column_mapping = {
        "reg_ans": "RegistroANS",
        "vl_saldo_final": "ValorDespesas"
    }

    missing = set(column_mapping.keys()) - set(df.columns)
    if missing:
        raise ValueError(
            f"Arquivo {csv_path.name} não possui colunas obrigatórias: {missing}"
        )

    df = df[list(column_mapping.keys())].rename(columns=column_mapping)

    # Normalização de tipos
    df["RegistroANS"] = df["RegistroANS"].astype(str).str.strip()

    df["ValorDespesas"] = (
        df["ValorDespesas"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )

    df["ValorDespesas"] = pd.to_numeric(
        df["ValorDespesas"],
        errors="coerce"
    )

    # Remove linhas inválidas
    df = df.dropna(subset=["RegistroANS", "ValorDespesas"])

    # Adiciona contexto do arquivo
    df["Ano"] = ano
    df["Trimestre"] = trimestre

    return df


# =========================
# CONSOLIDAÇÃO DOS TRIMESTRES
# =========================

def consolidate_quarters(csv_files: list[Path]) -> pd.DataFrame:
    """
    Consolida múltiplos CSVs trimestrais em um único DataFrame,
    extraindo Ano e Trimestre a partir do nome do arquivo.
    """

    dataframes = []

    for csv_file in csv_files:
        try:
            filename = csv_file.stem  # ex: "3T2025"

            # Extrai trimestre e ano do nome do arquivo
            trimestre = int(filename[0])
            ano = int(filename[2:6])

            df = read_and_clean_quarter_csv(
                csv_path=csv_file,
                ano=ano,
                trimestre=trimestre
            )

            dataframes.append(df)

        except Exception as e:
            print(f"Erro ao processar {csv_file.name}: {e}")

    if not dataframes:
        raise RuntimeError("Nenhum CSV trimestral válido foi processado.")

    consolidated = pd.concat(dataframes, ignore_index=True)

    return consolidated


# =========================
# LEITURA DO CADOP
# =========================

def read_cadop(cadop_csv: Path) -> pd.DataFrame:
    """
    Lê o CSV do CADOP e retorna um DataFrame normalizado
    com as colunas necessárias para enriquecimento dos dados.
    """

    df = pd.read_csv(
        cadop_csv,
        sep=";",
        encoding="latin1"
    )

    # 🔹 Normalização de colunas
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
    )

    # 🔹 Mapeamento para schema interno
    column_mapping = {
        "registro_operadora": "RegistroANS",
        "razao_social": "RazaoSocial",
        "cnpj": "CNPJ",
        "uf": "UF",
        "modalidade": "Modalidade"
    }

    missing = set(column_mapping.keys()) - set(df.columns)
    if missing:
        raise ValueError(
            f"CADOP não possui colunas obrigatórias: {missing}"
        )

    df = df[list(column_mapping.keys())].rename(columns=column_mapping)

    # 🔹 Normalizações finais
    df["RegistroANS"] = df["RegistroANS"].astype(str).str.strip()
    df["CNPJ"] = df["CNPJ"].astype(str).str.strip()
    df["RazaoSocial"] = df["RazaoSocial"].astype(str).str.strip()
    df["UF"] = df["UF"].astype(str).str.strip()
    df["Modalidade"] = df["Modalidade"].astype(str).str.strip()

    return df


# =========================
# JOIN DESPESAS + CADOP
# =========================

def enrich_with_cadop(
    despesas: pd.DataFrame,
    cadop: pd.DataFrame
) -> pd.DataFrame:
    """
    Enriquecimento via JOIN com o CADOP.
    """

    enriched = despesas.merge(
        cadop[
            [
                "RegistroANS",
                "RazaoSocial",
                "CNPJ",
                "UF",
                "Modalidade"
            ]
        ],
        on="RegistroANS",
        how="left"
    )

    return enriched


# =========================
# PIPELINE PRINCIPAL
# =========================

def run_transformation(
    quarterly_csvs: list[Path],
    cadop_csv: Path
) -> Path:
    """
    Executa todo o processo de transformação e retorna
    o caminho do CSV final gerado e zipa os arquivos.
    """

    print("Iniciando leitura e consolidação dos dados trimestrais...")
    despesas = consolidate_quarters(quarterly_csvs)

    print("Lendo cadastro de operadoras (CADOP)...")
    cadop = read_cadop(cadop_csv)

    print("Enriquecendo despesas com dados cadastrais...")
    final_df = enrich_with_cadop(despesas, cadop)

    output_path = PROCESSED_DIR / "consolidado_despesas.csv"
    final_df.to_csv(output_path, index=False, sep=";", encoding="utf-8-sig")

    zip_path = zip_output(output_path)

    print(f"CSV gerado: {output_path}")
    print(f"Arquivo zipado: {zip_path}")

    print("Agregando despesas por operadora e UF...")
    aggregated = aggregate_expenses(final_df)

    print("Agregando despesas finais...")
    aggregated = aggregate_expenses(final_df)

    aggregated_path = PROCESSED_DIR / "despesas_agregadas.csv"
    aggregated.to_csv(
        aggregated_path,
        index=False,
        sep=";",
        encoding="utf-8-sig"
    )

    print(f"Arquivo agregado gerado: {aggregated_path}")

    aggregated_csv = PROCESSED_DIR / "despesas_agregadas.csv"
    final_zip = PROCESSED_DIR / "Teste_Adriano_Gomes.zip"

    save_and_zip_aggregated(
        df=aggregated,
        csv_path=aggregated_csv,
        zip_path=final_zip
    )

    print(f"Arquivo final gerado e compactado: {final_zip}")


def zip_output(csv_path: Path) -> Path:
    zip_path = csv_path.with_suffix(".zip")

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(csv_path, arcname=csv_path.name)

    return zip_path


def aggregate_expenses(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega despesas por operadora, UF, ano e trimestre,
    calculando total e média trimestral.
    """

    required = {
        "CNPJ",
        "RazaoSocial",
        "UF",
        "Ano",
        "Trimestre",
        "ValorDespesas"
    }

    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Colunas obrigatórias ausentes para agregação: {missing}")

    aggregated = (
        df
        .groupby(
            ["CNPJ", "RazaoSocial", "UF", "Ano", "Trimestre"],
            as_index=False
        )
        .agg(
            DespesaTotal=("ValorDespesas", "sum"),
            DespesaMediaTrimestre=("ValorDespesas", "mean")
        )
        .sort_values("DespesaTotal", ascending=False)
    )

    # 🔹 Formatação monetária BRL
    aggregated["DespesaTotal"] = aggregated["DespesaTotal"].apply(format_brl)
    aggregated["DespesaMediaTrimestre"] = aggregated["DespesaMediaTrimestre"].apply(format_brl)

    return aggregated


def save_and_zip_aggregated(
    df: pd.DataFrame,
    csv_path: Path,
    zip_path: Path
) -> None:
    """
    Salva o CSV agregado e compacta em um ZIP.
    """

    csv_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(csv_path, index=False, sep=";", encoding="utf-8-sig")

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(csv_path, arcname=csv_path.name)