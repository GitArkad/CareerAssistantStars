from qdrant_client import models

def get_geo_filters(candidate: dict):
    """
    Гибкая фильтрация: Город (приоритет) -> Страна -> Без фильтров.
    """
    relocation = candidate.get("relocation", False)
    # Если разрешен переезд, фильтры не накладываем (ищем везде)
    if relocation:
        return []

    city_raw = candidate.get("city")
    country_raw = candidate.get("country")
    
    must_conditions = []

    # 1. ПРИОРИТЕТ: ГОРОД
    # Проверяем, что город указан и это не техническое слово "Remote"
    if city_raw and city_raw.lower().strip() != "remote":
        city = city_raw.strip().replace("'", "").replace('"', "")
        variants = list(set([city, city.lower(), city.capitalize()]))
        
        must_conditions.append(
            models.FieldCondition(
                key="city", 
                match=models.MatchAny(any=variants)
            )
        )
    
    # 2. ЗАПАСНОЙ ВАРИАНТ: СТРАНА
    # Если города нет, но указана страна (и это не "Remote")
    elif country_raw and country_raw.lower().strip() != "remote":
        country = country_raw.strip().replace("'", "").replace('"', "")
        variants = list(set([country, country.lower(), country.capitalize()]))
        
        must_conditions.append(
            models.FieldCondition(
                key="country", 
                match=models.MatchAny(any=variants)
            )
        )
        
    return must_conditions