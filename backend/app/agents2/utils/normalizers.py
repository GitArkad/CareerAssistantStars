# utils/normalizers.py

import re
from app.agents2.utils.city_map import CITY_NORMALIZATION_MAP
from app.agents2.utils.country_map import COUNTRY_NORMALIZATION_MAP


def _clean(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    return text


def _to_title(text: str) -> str:
    if not text:
        return None
    return " ".join(word.capitalize() for word in text.split())


def normalize_city(city: str) -> str:
    if not city:
        return None

    city_clean = _clean(city)

    normalized = CITY_NORMALIZATION_MAP.get(city_clean)

    if normalized:
        return normalized 

    return _to_title(city_clean)


def normalize_country(country: str) -> str:
    if not country:
        return None

    country_clean = _clean(country)

    normalized = COUNTRY_NORMALIZATION_MAP.get(country_clean)

    if normalized:
        return normalized 

    return _to_title(country_clean)