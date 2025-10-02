import re
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE = "https://www.leylobby.gob.cl/instituciones"


def get_soup(session, url):
    r = session.get(url, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml"), r.text, r.url  # devuelvo la URL efectiva


def read_table_from_html(html_text):
    # Busca la tabla con columnas Servicio y Url
    tables = pd.read_html(html_text)
    for df in tables:
        cols = [c.strip().lower() for c in df.columns.astype(str)]
        if {"servicio", "url"}.issubset(set(cols)):
            df.columns = cols
            return df[["servicio", "url"]].copy()
    raise ValueError(
        "No se encontró la tabla esperada con columnas 'Servicio' y 'Url'."
    )


def find_next_url(soup, current_url):
    """
    Intenta encontrar el enlace de siguiente página:
    1) <a rel="next"> si existe
    2) un <a> cuyo texto sea '»' o 'Siguiente'
    """
    a = soup.find("a", attrs={"rel": "next"})
    if a and a.get("href"):
        return urljoin(current_url, a["href"])
    for a in soup.find_all("a", href=True):
        txt = (a.get_text() or "").strip()
        if txt in {"»", "Siguiente"}:
            return urljoin(current_url, a["href"])
    return None


def scrape_instituciones(sleep_secs=0.4, max_pages=None, verbose=False):
    out = []
    with requests.Session() as s:
        s.headers.update({"User-Agent": "Mozilla/5.0 (educational scraping)"})
        # página inicial
        soup, html, cur = get_soup(s, BASE)
        page_count = 0
        while True:
            page_count += 1
            if verbose:
                print(f"[info] página {page_count}: {cur}")
            df = read_table_from_html(html)
            out.append(df)

            if max_pages and page_count >= max_pages:
                break

            # buscar link a la siguiente
            next_url = find_next_url(soup, cur)
            if not next_url:
                break

            soup, html, cur = get_soup(s, next_url)
            time.sleep(sleep_secs)

    all_df = pd.concat(out, ignore_index=True).drop_duplicates()
    all_df["codigo"] = all_df["url"].str.extract(r"/instituciones/([A-Z0-9]+)")
    final_df = (
        all_df.rename(columns={"servicio": "nombre"})[["codigo", "nombre"]]
        .sort_values(["codigo", "nombre"])
        .reset_index(drop=True)
    )
    return final_df


# Ejemplo:
# instituciones = scrape_instituciones(verbose=True)
# print(instituciones.shape, instituciones.head())
# instituciones.to_csv("instituciones_lobby.csv", index=False)
