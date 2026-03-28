# # app/agents2/services/vacancy_search.py

# app/agents2/services/vacancy_search.py
from qdrant_client.models import Filter, FieldCondition, MatchValue, MatchAny

def search_vacancies(candidate: dict, client):
    # 1. Подготовка текстового запроса (для семантики)
    # Используем навыки и специализацию
    query_parts = [candidate.get("specialization", "")]
    query_parts.extend(candidate.get("skills", []))
    
    # Префикс 'query: ' обязателен для модели E5 при поиске
    query_text = f"query: {' '.join(filter(None, query_parts)).strip()}"

    # 2. Логика фильтрации
    city = candidate.get("city_normalized")
    relocation = candidate.get("relocation", False)

    query_filter = None

    if city:
        if not relocation:
            # 🔴 БЕЗ РЕЛОКАЦИИ: Строго город из профиля + любая удаленка
            query_filter = Filter(
                must=[
                    FieldCondition(
                        key="city",
                        match=MatchAny(any=[city, "REMOTE"])
                    )
                ]
            )
            print(f"--- [FILTER] Strict: {city} or REMOTE ---")
        else:
            # 🟢 С РЕЛОКАЦИЕЙ: Весь мир (фильтр не создаем)
            query_filter = None
            print("--- [FILTER] Disabled: Global search enabled ---")

    # 3. Выполнение поиска
    # Используем query_points (рекомендуемый метод для новых версий Qdrant)
    results = client.query_points(
        collection_name="vacancies",
        query=query_text, 
        limit=5,
        query_filter=query_filter
    )

    # 4. Debug: что реально вернулось из базы
    for p in results.points:
        p_city = p.payload.get('city')
        p_title = p.payload.get('title')
        print(f"MATCH: {p_title} in {p_city} (Score: {p.score:.4f})")

    return results

# from qdrant_client.models import Filter, FieldCondition, MatchValue


# def search_vacancies(candidate: dict, client):
#     # -----------------------------
#     # 1. Формируем query (пока не используем)
#     # -----------------------------
#     query_parts = []

#     if candidate.get("specialization"):
#         query_parts.append(candidate["specialization"])

#     if candidate.get("skills"):
#         query_parts.extend(candidate["skills"])

#     query = " ".join(query_parts).strip()

#     # -----------------------------
#     # 2. Данные кандидата
#     # -----------------------------
#     city = candidate.get("city_normalized")
#     relocation = candidate.get("relocation", True)

#     work_format = [w.lower() for w in candidate.get("work_format", [])]

#     # 🔥 фикс: если пусто — считаем office
#     if not work_format:
#         work_format = ["office"]

#     print("QUERY:", query)
#     print("CITY_NORMALIZED:", city)
#     print("RELOCATION:", relocation)
#     print("WORK_FORMAT:", work_format)

#     # -----------------------------
#     # 3. ФИЛЬТР (жёсткий, чтобы точно работал)
#     # -----------------------------
#     query_filter = None

#     if city:
#         # 🔴 без релокации → строго город
#         if not relocation:
#             query_filter = Filter(
#                 must=[
#                     FieldCondition(
#                         key="city",  # ⚠️ проверь что поле реально так называется
#                         match=MatchValue(value=city)
#                     )
#                 ]
#             )

#         # 🟢 с релокацией → город ИЛИ remote (но пока упростим)
#         else:
#             query_filter = Filter(
#                 must=[
#                     FieldCondition(
#                         key="city",
#                         match=MatchValue(value=city)
#                     )
#                 ]
#             )

#     print("FILTER:", query_filter)

#     # -----------------------------
#     # 4. Поиск (ВАЖНО: без query)
#     # -----------------------------
#     results = client.query_points(
#         collection_name="vacancies",
#         query=None,  # 🔥 КЛЮЧЕВОЙ ФИКС
#         limit=5,
#         query_filter=query_filter
#     )

#     # -----------------------------
#     # 5. Debug payload (очень полезно)
#     # -----------------------------
#     try:
#         for p in results.points:
#             print("PAYLOAD:", p.payload)
#     except Exception:
#         pass

#     # # -----------------------------
#     # # 6. Fallback (если вообще ничего)
#     # # -----------------------------
#     # if not results or len(results.points) == 0:
#     #     print("No results — fallback to global search")

#     #     results = client.query_points(
#     #         collection_name="vacancies",
#     #         query=None,
#     #         limit=5
#     #     )

#     return results