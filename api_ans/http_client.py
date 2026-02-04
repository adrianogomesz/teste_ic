#Imports
import requests

def get_url(url: str) -> str:
    """
    Faz uma requisição GET para a URL especificada e retorna o conteúdo como texto.
    """
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        print(f"Requisição bem-sucedida: {url}")
        return response.text
    except requests.RequestException as e:
        print(f"Erro ao acessar a URL {url}: {e}")
        raise