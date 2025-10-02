import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup

BASE = "https://www.leylobby.gob.cl"
HDRS = {"User-Agent": "Mozilla/5.0 (educational scraping)"}


def _get_soup(url, *, params=None, timeout=30, session=None):
    s = session or requests.Session()
    r = s.get(url, params=params, headers=HDRS, timeout=timeout)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml"), s


def _get_years(codigo, session):
    url = f"{BASE}/instituciones/{codigo}/audiencias"
    soup, _ = _get_soup(url, session=session)
    years = []
    for a in soup.find_all("a", href=True):
        m = re.search(r"/audiencias/(\d{4})$", a["href"])
        if m:
            years.append(int(m.group(1)))
    return sorted(set(years))


def _get_nombre_desde_pagina_sujeto(session, codigo, year, sid):
    """Lee el <h2> 'Audiencias - Año AAAA - NOMBRE' y devuelve NOMBRE."""
    url = f"{BASE}/instituciones/{codigo}/audiencias/{year}/{sid}"
    soup, _ = _get_soup(url, session=session)
    for tag in soup.find_all(["h1", "h2", "h3"]):
        t = tag.get_text(" ", strip=True)
        m = re.search(r"Audiencias\s*-\s*Año\s*\d{4}\s*-\s*(.+)$", t, flags=re.I)
        if m:
            return m.group(1).strip()
    return None  # fallback si cambia el sitio


def _parse_year_listing(html_soup, session, codigo, year, sleep_secs=0.2):
    """
    En /audiencias/{AAAA} cada fila tiene:
      NOMBRE   CARGO   [Ver Detalle]
    Tomamos el id desde 'Ver Detalle', sacamos el NOMBRE de su página,
    y el CARGO lo obtenemos restándole el prefijo del nombre a la línea.
    """
    rows = []
    for a in html_soup.find_all("a"):
        text = (a.get_text() or "").strip().lower()
        if not text.startswith("ver detalle"):
            continue

        href = a.get("href") or ""
        m = re.search(r"/audiencias/\d{4}/(\d+)$", href)
        if not m:
            continue
        sid = m.group(1)

        # contenedor textual de la fila
        container = a.parent
        hops = 0
        while container and len(container.get_text(" ", strip=True)) < 20 and hops < 3:
            container = container.parent
            hops += 1
        if not container:
            continue

        # texto antes de 'Ver Detalle' = "NOMBRE ... CARGO ..."
        line = container.get_text(" ", strip=True)
        prefix = line.split("Ver Detalle", 1)[0].strip()

        # 1) nombre confiable desde la página del sujeto
        nombre = _get_nombre_desde_pagina_sujeto(session, codigo, year, sid)
        time.sleep(sleep_secs)
        if not nombre:
            # si no encontramos el h2 (caso raro), caemos a un split básico
            parts = prefix.split()
            if len(parts) >= 2:
                rows.append(
                    {"nombre": parts[0], "cargo": " ".join(parts[1:]), "id": sid}
                )
            continue

        # 2) cargo = prefix - nombre (por tokens; robusto a espacios)
        pref_tokens = prefix.split()
        name_tokens = nombre.split()
        cargo = None
        if pref_tokens[: len(name_tokens)] == name_tokens and len(pref_tokens) > len(
            name_tokens
        ):
            cargo = " ".join(pref_tokens[len(name_tokens) :])
        else:
            # intento regex con nombre literal al inicio (case-insensitive)
            m2 = re.match(rf"^{re.escape(nombre)}\s+(?P<cargo>.+)$", prefix, flags=re.I)
            cargo = m2.group("cargo").strip() if m2 else None

        rows.append({"nombre": nombre, "cargo": cargo or "", "id": sid})

    return pd.DataFrame(rows)


def scrape_audiencias_subjects(
    codigo: str, years: list[int] | None = None, sleep_secs: float = 0.5
) -> pd.DataFrame:
    """
    Devuelve DataFrame con columnas: nombre, cargo, id
    Recorre todos los años activos (o los indicados en 'years').
    """
    out = []
    with requests.Session() as session:
        if years is None:
            years = _get_years(codigo, session)
        for year in years:
            url = f"{BASE}/instituciones/{codigo}/audiencias/{year}"
            soup, _ = _get_soup(url, session=session)
            df = _parse_year_listing(
                soup, session, codigo, year, sleep_secs=min(0.2, sleep_secs)
            )
            if not df.empty:
                df["anio"] = year
                out.append(df)
            time.sleep(sleep_secs)

    if not out:
        return pd.DataFrame(columns=["nombre", "cargo", "id"])
    res = (
        pd.concat(out, ignore_index=True)
        .drop_duplicates(subset=["id"])
        .reset_index(drop=True)
    )
    return res[["nombre", "cargo", "id"]]
