"""
Модуль нормализации географических названий для поиска вакансий.

Импортирует карты нормализации из отдельных файлов:
- city_map.py: CITY_NORMALIZATION_MAP
- country_map.py: COUNTRY_NORMALIZATION_MAP

Примеры использования:
    >>> normalize_city("москва")
    'MOSCOW'
    >>> normalize_city("Санкт-Петербург")
    'SAINT PETERSBURG'
    >>> normalize_country("россия")
    'RUSSIA'
"""

from .city_map import CITY_NORMALIZATION_MAP
from .country_map import COUNTRY_NORMALIZATION_MAP


def normalize_city(city: str) -> str:
    """
    Приводит название города к нормализованному виду.
    
    Алгоритм:
    1. Проверяет входное значение на пустоту
    2. Приводит к нижнему регистру и удаляет пробелы по краям
    3. Ищет в карте нормализации CITY_NORMALIZATION_MAP
    4. Если найдено — возвращает нормализованное значение
    5. Если не найдено — возвращает оригинал в верхнем регистре
    
    Args:
        city: Название города в любом формате (например, "москва", "Москва", "MOSCOW")
    
    Returns:
        Нормализованное название города (например, "MOSCOW")
        или пустую строку если вход пустой
    
    Examples:
        >>> normalize_city("москва")
        'MOSCOW'
        >>> normalize_city("Санкт-Петербург")
        'SAINT PETERSBURG'
        >>> normalize_city("MOSCOW")
        'MOSCOW'
        >>> normalize_city("")
        ''
        >>> normalize_city(None)
        ''
    """
    if not city:
        return ""
    
    normalized = city.lower().strip()
    return CITY_NORMALIZATION_MAP.get(normalized, city.upper())


def normalize_country(country: str) -> str:
    """
    Приводит название страны к нормализованному виду.
    
    Алгоритм:
    1. Проверяет входное значение на пустоту
    2. Приводит к нижнему регистру и удаляет пробелы по краям
    3. Ищет в карте нормализации COUNTRY_NORMALIZATION_MAP
    4. Если найдено — возвращает нормализованное значение
    5. Если не найдено — возвращает оригинал в верхнем регистре
    
    Args:
        country: Название страны в любом формате (например, "россия", "Россия", "RUSSIA")
    
    Returns:
        Нормализованное название страны (например, "RUSSIA")
        или пустую строку если вход пустой
    
    Examples:
        >>> normalize_country("россия")
        'RUSSIA'
        >>> normalize_country("Казахстан")
        'KAZAKHSTAN'
        >>> normalize_country("USA")
        'UNITED STATES'
        >>> normalize_country("")
        ''
        >>> normalize_country(None)
        ''
    """
    if not country:
        return ""
    
    normalized = country.lower().strip()
    return COUNTRY_NORMALIZATION_MAP.get(normalized, country.upper())


def get_city_aliases(city_name: str) -> list:
    """
    Возвращает все возможные алиасы для города.
    
    Полезно для отладки или для создания расширенных фильтров поиска.
    
    Args:
        city_name: Нормализованное название города (например, "MOSCOW")
    
    Returns:
        Список всех алиасов, которые маппятся на этот город
    
    Examples:
        >>> get_city_aliases("MOSCOW")
        ['москва', 'moscow', 'msk', 'мск']
    """
    if not city_name:
        return []
    
    target = city_name.upper()
    return [
        alias for alias, normalized 
        in CITY_NORMALIZATION_MAP.items() 
        if normalized.upper() == target
    ]


def get_country_aliases(country_name: str) -> list:
    """
    Возвращает все возможные алиасы для страны.
    
    Args:
        country_name: Нормализованное название страны (например, "RUSSIA")
    
    Returns:
        Список всех алиасов, которые маппятся на эту страну
    
    Examples:
        >>> get_country_aliases("RUSSIA")
        ['россия', 'рф', 'russian federation', 'ru']
    """
    if not country_name:
        return []
    
    target = country_name.upper()
    return [
        alias for alias, normalized 
        in COUNTRY_NORMALIZATION_MAP.items() 
        if normalized.upper() == target
    ]


# =============================================================================
# ЭКСПОРТЫ
# =============================================================================

__all__ = [
    "normalize_city",
    "normalize_country",
    "get_city_aliases",
    "get_country_aliases",
    "CITY_NORMALIZATION_MAP",
    "COUNTRY_NORMALIZATION_MAP",
]