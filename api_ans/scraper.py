from bs4 import BeautifulSoup
from urllib.parse import urljoin

from api_ans.http_client import get_url
from api_ans.utils import extract_quarter


def extract_hrefs(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    return [
        a.get("href")
        for a in soup.find_all("a")
        if a.get("href")
    ]


def find_dir(base_url: str) -> str:
    html = get_url(base_url)
    hrefs = extract_hrefs(html)

    for href in hrefs:
        name = href.lower()
        if "demonstr" in name and "contab" in name:
            return urljoin(base_url, href)

    raise RuntimeError("Pasta de Demonstrações Contábeis não localizada.")


def list_years(demo_url: str) -> list[str]:
    html = get_url(demo_url)
    hrefs = extract_hrefs(html)

    return [
        href
        for href in hrefs
        if href.endswith("/") and href[:-1].isdigit()
    ]


def list_zip_files(year_url: str) -> list[str]:
    html = get_url(year_url)
    hrefs = extract_hrefs(html)

    return [
        href for href in hrefs
        if href.endswith(".zip")
    ]


def find_last_three_quarters(demo_url: str) -> list[str]:
    years = sorted(list_years(demo_url), reverse=True)

    selected = []

    for year in years:
        year_url = urljoin(demo_url, year)
        zips = list_zip_files(year_url)

        zips.sort(key=extract_quarter, reverse=True)

        for zip_file in zips:
            selected.append(urljoin(year_url, zip_file))
            if len(selected) == 3:
                return selected

    return selected
    