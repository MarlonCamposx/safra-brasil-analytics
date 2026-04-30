import re
from typing import Optional

_CROP_PREFIXES = {
    "milho": "milho",
    "soja": "soja",
    "trigo": "trigo",
    "algodão": "algodão",
    "arroz": "arroz",
    "feijão": "feijão",
    "sorgo": "sorgo",
    "cana": "cana-de-açúcar",
}


def extract_harvest_year(safra) -> int:
    if safra is None:
        raise ValueError("formato inválido: safra não pode ser None")
    s = str(safra).strip()
    if re.fullmatch(r"\d{4}", s):
        return int(s)
    m = re.fullmatch(r"(\d{4})/(\d{2})", s)
    if not m:
        raise ValueError(f"formato inválido: '{s}'")
    start_year = int(m.group(1))
    suffix = int(m.group(2))
    start_century = start_year // 100
    start_last2 = start_year % 100
    century = start_century + 1 if suffix < start_last2 else start_century
    return century * 100 + suffix


def normalize_crop_name(name: str) -> str:
    lower = name.lower()
    for prefix, normalized in _CROP_PREFIXES.items():
        if lower.startswith(prefix):
            return normalized
    # strip parenthetical suffixes (e.g. "soja (em grão)")
    base = re.sub(r"\s*\(.*?\)", "", lower).strip()
    for prefix, normalized in _CROP_PREFIXES.items():
        if base.startswith(prefix):
            return normalized
    return base


def calculate_yield(
    production_ton: Optional[float], harvested_area_ha: Optional[float]
) -> Optional[float]:
    if production_ton is None or harvested_area_ha is None:
        return None
    if harvested_area_ha == 0:
        return None
    return production_ton / harvested_area_ha
