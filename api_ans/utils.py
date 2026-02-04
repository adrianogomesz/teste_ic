# Imports
import os, zipfile, shutil
import pandas as pd


def extract_quarter(filename: str) -> int:
    # "3T2024.zip" → 3
    return int(filename[0])


def zip_processed_file(source_csv_path: str, output_zip_path: str):
    """
    Compacta um arquivo CSV em um arquivo ZIP.
    """
    if os.path.exists(source_csv_path):
        try:
            with zipfile.ZipFile(output_zip_path, 'w', zipfile.ZIP_DEFLATED) as z:
                z.write(source_csv_path, arcname=os.path.basename(source_csv_path))
            print(f"Sucesso! Arquivo compactado em: {output_zip_path}")
            
            # Opcional: remover o .csv original após zipar
            # os.remove(source_csv_path) 
            
        except Exception as e:
            print(f"Erro ao compactar o arquivo: {e}")
    else:
        print(f"Erro: O arquivo {source_csv_path} não foi encontrado para ser zipado.")


def format_brl(value: float) -> str:
    """
    Formata um valor numérico para moeda BRL.
    Ex: 1234567.89 -> R$ 1.234.567,89
    """
    if pd.isna(value):
        return "R$ 0,00"

    return (
        f"R$ {value:,.2f}"
        .replace(",", "X")
        .replace(".", ",")
        .replace("X", ".")
    )