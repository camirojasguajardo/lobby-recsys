import re
import time
from typing import List, Dict, Tuple
from urllib.parse import urljoin, urlparse

import requests
import pandas as pd
from bs4 import BeautifulSoup

BASE = "https://www.leylobby.gob.cl"

SESSION = requests.Session()
SESSION.headers.update(
    {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
    }
)


def _text(s):
    return re.sub(r"\s+", " ", (s or "")).strip()


def _get_soup(url: str, max_retries: int = 3, timeout: int = 25):
    print(url)
    # añade Accept-Language por si el sitio hace content-negotiation
    SESSION.headers.setdefault("Accept-Language", "es-CL,es;q=0.9,en;q=0.8")
    for i in range(max_retries):
        r = SESSION.get(url, timeout=timeout)
        if r.status_code == 200:
            return BeautifulSoup(r.text, "html.parser")
        if r.status_code in (429, 502, 503, 504):
            time.sleep(2 * (i + 1))
            continue
        r.raise_for_status()
    raise RuntimeError(f"No pude obtener: {url}")


def _extract_detalle_links(soup: BeautifulSoup):
    links = []
    # 1) regex: /instituciones/<id>/audiencias/<anio>/<sp>/<audiencia_id>(/...)?
    pat = re.compile(r"^/instituciones/[^/]+/audiencias/\d{4}(?:/\d+)+/?$")

    for a in soup.find_all("a", href=True):
        href_raw = a["href"].strip()

        # Normaliza: si es absoluta -> quédate con .path; si es relativa sin "/" agrégalo
        if href_raw.startswith("http://") or href_raw.startswith("https://"):
            p = urlparse(href_raw)
            path = p.path or "/"
        else:
            path = href_raw if href_raw.startswith("/") else "/" + href_raw

        # Quita hashbang si aparece (#!/...)
        if path.startswith("/#!/"):
            path = path[3:]  # "/#!/x" -> "/x"

        if pat.match(path):
            links.append(urljoin(BASE, path))

        # Fallback: algunos “Ver Detalle” podrían no calzar estrictamente con el patrón;
        # si el texto del anchor contiene "Ver Detalle", lo tomamos igual.
        else:
            txt = a.get_text(" ", strip=True)
            if "ver detalle" in txt.lower():
                links.append(urljoin(BASE, href_raw))

    # opcional: dedup y log de ejemplo
    out = sorted(set(links))
    print(f"[debug] {len(out)} links a detalle detectados. Ejemplo: {out[:3]}")
    return out


def _list_url(institucion_id: str, anio: int, sujeto_pasivo_id: str | int) -> str:
    return f"{BASE}/instituciones/{institucion_id}/audiencias/{anio}/{sujeto_pasivo_id}"


"""def _extract_detalle_links(soup: BeautifulSoup) -> List[str]:
    links = []
    for a in soup.find_all("a", string=lambda s: s and "Ver Detalle" in s):
        href = a.get("href")
        if href and href.startswith("/instituciones/"):
            links.append(urljoin(BASE, href))
    print(links)
    return sorted(set(links))"""


def _find_section(soup: BeautifulSoup, prefix: str):
    for h in soup.find_all(re.compile("^h[1-3]$")):
        if _text(h.get_text(" ")).lower().startswith(prefix.lower()):
            return h
    return None


def _parse_info_general(h) -> Dict[str, str]:
    if h is None:
        return {}
    labels = ["Identificador", "Fecha", "Forma", "Lugar", "Duración"]
    want = {k: "" for k in labels}
    expecting: str | None = None

    def is_header(tag) -> bool:
        return bool(re.match(r"^h[1-3]$", (tag.name or "")))

    for el in h.find_all_next():
        if is_header(el):  # fin de la sección
            break
        if el.name not in ("p", "div", "li", "span", "strong", "label", "td"):
            continue

        txt = _text(el.get_text(" "))
        if not txt:
            continue

        # Caso 1: "Etiqueta: Valor" en el mismo nodo
        m_inline = re.match(
            r"^(Identificador|Fecha|Forma|Lugar|Duración)\s*:?\s+(.+)$", txt, flags=re.I
        )
        if m_inline:
            key = m_inline.group(1).capitalize()
            val = m_inline.group(2).strip()
            want[key] = val
            expecting = None
            continue

        # Caso 2: "Etiqueta" sola -> el próximo bloque "no vacío" es su valor
        m_label_only = re.match(
            r"^(Identificador|Fecha|Forma|Lugar|Duración)\s*:?\s*$", txt, flags=re.I
        )
        if m_label_only:
            expecting = m_label_only.group(1).capitalize()
            continue

        # Si venimos esperando el valor de alguna etiqueta, toma este bloque como valor
        if (
            expecting
            and (expecting in want)
            and not re.match(
                r"^(Identificador|Fecha|Forma|Lugar|Duración)\s*:?\s*$", txt, flags=re.I
            )
        ):
            want[expecting] = txt
            expecting = None

    return {k: v for k, v in want.items() if v}


def _parse_asistentes(h) -> List[Dict[str, str]]:
    if h is None:
        return []
    # buscar tabla de asistentes
    table = None
    for sib in h.find_all_next():
        if re.match(r"^h[1-3]$", sib.name or ""):
            break
        if sib.name == "table":
            table = sib
            break

    out = []
    if table:
        headers = [_text(th.get_text(" ")) for th in table.find_all("th")]
        if not headers:
            first = table.find("tr")
            if first:
                headers = [
                    _text(td.get_text(" ")) for td in first.find_all(["td", "th"])
                ]
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            row = {}
            for i, td in enumerate(tds):
                key = headers[i] if i < len(headers) and headers[i] else f"col_{i+1}"
                row[key] = _text(td.get_text(" "))
            if any(v for v in row.values()):
                out.append(row)
    else:
        # fallback: lista simple
        ul = h.find_next("ul")
        if ul:
            for li in ul.find_all("li"):
                out.append({"Asistente": _text(li.get_text(" "))})
    return out


def _parse_text_block(h) -> str:
    if h is None:
        return ""
    texts = []
    for sib in h.find_all_next():
        if re.match(r"^h[1-3]$", sib.name or ""):
            break
        if sib.name in ("p", "div", "span", "li"):
            t = _text(sib.get_text(" "))
            if t:
                texts.append(t)
    # normalmente es un párrafo; devolvemos el primero significativo
    return next((t for t in texts if t), "")


def _parse_detalle(url_detalle: str) -> Tuple[Dict, List[Dict], Dict]:
    soup = _get_soup(url_detalle)
    h_info = _find_section(soup, "1. Información General")
    h_asist = _find_section(soup, "2. Asistentes")
    h_mat = _find_section(soup, "3. Materias")
    h_esp = _find_section(soup, "4. Especificación")

    info = _parse_info_general(h_info)
    asistentes = _parse_asistentes(h_asist)
    materias = {
        "materias_tratadas": _parse_text_block(h_mat),
        "especificacion": _parse_text_block(h_esp),
    }

    audiencia_id = url_detalle.rstrip("/").split("/")[-1]
    info["audiencia_id"] = audiencia_id
    info["detalle_url"] = url_detalle
    return info, asistentes, materias


def scrape_audiencias_dataframes(
    institucion_id: str,
    sujeto_pasivo_ids: List[int | str],
    anios: List[int],
    pause_seconds: float = 0.8,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    audiencias_rows: List[Dict] = []
    asistentes_rows: List[Dict] = []

    for sp_id in sujeto_pasivo_ids:
        for anio in anios:
            index_url = _list_url(institucion_id, anio, sp_id)
            try:
                soup = _get_soup(index_url)
            except Exception as e:
                print(f"[WARN] No abre índice {index_url}: {e}")
                continue

            detalle_links = _extract_detalle_links(soup)
            print(f"{institucion_id}-{sp_id}-{anio}: {len(detalle_links)} audiencias")

            for href in detalle_links:
                try:
                    info, asistentes, materias = _parse_detalle(href)
                except Exception as e:
                    print(f"[WARN] Error detalle {href}: {e}")
                    continue

                # audiencias
                base_info = {
                    "institucion_id": institucion_id,
                    "sujeto_pasivo_id": str(sp_id),
                    "anio": anio,
                    **info,
                    **materias,
                }
                audiencias_rows.append(base_info)

                # asistentes
                for a in asistentes:
                    asistentes_rows.append(
                        {
                            "audiencia_id": info["audiencia_id"],
                            "institucion_id": institucion_id,
                            "sujeto_pasivo_id": str(sp_id),
                            "anio": anio,
                            **a,
                        }
                    )

                time.sleep(pause_seconds)

            time.sleep(pause_seconds + 0.4)

    # DataFrames
    audiencias_df = pd.DataFrame(audiencias_rows).drop_duplicates(
        subset=["audiencia_id"], keep="last"
    )
    asistentes_df = pd.DataFrame(asistentes_rows)

    # Normaliza nombres esperados si existen
    rename_map = {
        "Identificador": "identificador",
        "Fecha": "fecha",
        "Forma": "forma",
        "Lugar": "lugar",
        "Duración": "duracion",
    }
    audiencias_df = audiencias_df.rename(columns=rename_map)

    # Tipos útiles
    if "fecha" in audiencias_df.columns:
        # muchas veces viene en formato DD/MM/AAAA
        audiencias_df["fecha"] = pd.to_datetime(
            audiencias_df["fecha"], dayfirst=True, errors="coerce"
        )

    return audiencias_df, asistentes_df
