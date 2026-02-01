# Imports
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# Url da API
BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/"

# Funções
def get_url(url: str) -> str:
    
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    return response.text
    

html = get_url(BASE_URL)

soup = BeautifulSoup(html, 'html.parser')

print(html)

