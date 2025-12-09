import pandas as pd
import re


def _norm_text(s):
    if pd.isna(s):
        return ""
    s = str(s).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _pick_org(row):
    rep = row.get("Representa a", "")
    tra = row.get("Trabaja para", "")
    return (
        rep
        if (isinstance(rep, str) and rep.strip())
        else (tra if isinstance(tra, str) else "")
    )
