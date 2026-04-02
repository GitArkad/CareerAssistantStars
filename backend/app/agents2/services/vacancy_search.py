# app/agents2/services/vacancy_search.py

from qdrant_client.models import Filter, FieldCondition, MatchValue


def search_vacancies(candidate: dict, client):
    # -----------------------------
    # 1. Формируем query
    # -----------------------------
    query_parts = []

    if candidate.get("specialization"):
        query_parts.append(candidate["specialization"])

    if candidate.get("skills"):
        query_parts.extend(candidate["skills"])

    query = " ".join(query_parts)

    # -----------------------------
    # 2. Город (ожидаем EN из парсера)
    # -----------------------------
    city = candidate.get("city")

    print("QUERY:", query)
    print("CITY:", city)

    # -----------------------------
    # 3. Фильтр:
    # (city совпадает ИЛИ вакансия remote)
    # -----------------------------
    query_filter = None

    if city:
        query_filter = Filter(
            should=[
                # локальные вакансии
                FieldCondition(
                    key="city",
                    match=MatchValue(value=city)
                ),
                # удалённые вакансии
                FieldCondition(
                    key="is_remote",
                    match=MatchValue(value=True)
                )
            ]
        )

    # -----------------------------
    # 4. Основной поиск
    # -----------------------------
    results = client.query_points(
        collection_name="vacancies",
        query=query,
        limit=5,
        query_filter=query_filter
    )

    # -----------------------------
    # 5. Fallback (если пусто)
    # -----------------------------
    if not results:
        print("No results — fallback to global search")

        results = client.query_points(
            collection_name="vacancies",
            query=query,
            limit=5
        )

    return results