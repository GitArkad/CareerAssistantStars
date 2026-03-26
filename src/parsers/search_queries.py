"""
search_queries.py

Exhaustive IT job search queries — English + Russian.
Keeps the full master list, but also supports a focused/core mode for
regular production-style runs with less noise and lower API load.

Usage:
    from search_queries import QUERIES_EN, QUERIES_RU, ALL_QUERIES_EN
    from search_queries import CATEGORY_MAP, get_queries_for_source
    from search_queries import CORE_CATEGORIES, get_core_queries_for_source
"""

from __future__ import annotations

import os

# ============================================================================
# ENGLISH QUERIES (for Adzuna, USAJobs, Himalayas, Arbeitnow, ATS APIs)
# ============================================================================

QUERIES_EN = {
    # ── Data Science & ML ──
    "data_science": [
        "Data Scientist", "Senior Data Scientist", "Junior Data Scientist",
        "Lead Data Scientist", "Staff Data Scientist", "Principal Data Scientist",
        "Applied Data Scientist", "Quantitative Scientist",
    ],
    "machine_learning": [
        "Machine Learning Engineer", "ML Engineer", "AI Engineer",
        "AI/ML Engineer", "Artificial Intelligence Engineer",
        "Deep Learning Engineer", "Applied ML Engineer",
        "ML Platform Engineer", "ML Infrastructure Engineer",
        "Machine Learning Scientist",
    ],
    "nlp_llm": [
        "NLP Engineer", "Natural Language Processing Engineer",
        "LLM Engineer", "Conversational AI Engineer",
        "Generative AI Engineer", "GenAI Engineer",
        "Prompt Engineer", "AI Solutions Engineer",
    ],
    "computer_vision": [
        "Computer Vision Engineer", "Image Processing Engineer",
        "Perception Engineer", "3D Vision Engineer",
    ],
    "research": [
        "Research Scientist", "Research Engineer", "Applied Scientist",
        "Applied Research Scientist", "ML Research Engineer",
        "AI Researcher", "Quantitative Researcher",
    ],

    # ── Data Engineering ──
    "data_engineering": [
        "Data Engineer", "Senior Data Engineer", "Big Data Engineer",
        "ETL Developer", "ELT Developer", "Data Pipeline Engineer",
        "Data Platform Engineer", "Data Infrastructure Engineer",
        "Streaming Data Engineer", "Hadoop Developer",
        "Spark Developer", "Kafka Developer",
    ],
    "data_architecture": [
        "Data Architect", "Cloud Data Architect",
        "Enterprise Data Architect", "Solutions Architect",
        "Data Modeling Engineer", "Database Architect",
    ],
    "analytics_engineering": [
        "Analytics Engineer", "dbt Developer",
        "Data Transformation Engineer", "Data Warehouse Engineer",
    ],

    # ── Analytics & BI ──
    "analytics": [
        "Data Analyst", "Senior Data Analyst", "Junior Data Analyst",
        "Business Analyst", "Product Analyst", "Marketing Analyst",
        "Financial Analyst", "Operations Analyst", "Growth Analyst",
        "Pricing Analyst", "Risk Analyst", "Fraud Analyst",
        "Web Analyst", "CRM Analyst",
    ],
    "bi": [
        "BI Developer", "BI Analyst", "BI Engineer",
        "Business Intelligence Developer", "Business Intelligence Analyst",
        "Tableau Developer", "Power BI Developer", "Looker Developer",
        "Reporting Analyst", "SQL Analyst", "Analytics Manager",
    ],

    # ── Backend Development ──
    "backend": [
        "Backend Developer", "Backend Engineer",
        "Python Developer", "Python Backend Developer",
        "Java Developer", "Java Backend Developer",
        "Go Developer", "Golang Developer",
        "Node.js Developer", "Node Developer",
        "Ruby Developer", "Ruby on Rails Developer",
        "PHP Developer", "Laravel Developer",
        "Rust Developer", ".NET Developer", "C# Developer",
        "Scala Developer", "Kotlin Developer",
        "API Developer", "Microservices Developer",
    ],
    "fullstack": [
        "Full Stack Developer", "Fullstack Developer",
        "Full Stack Engineer", "Fullstack Engineer",
        "Software Engineer", "Software Developer",
        "Web Developer", "Application Developer",
    ],

    # ── Frontend Development ──
    "frontend": [
        "Frontend Developer", "Frontend Engineer",
        "Front-end Developer", "Front End Developer",
        "React Developer", "React Engineer",
        "Vue.js Developer", "Vue Developer",
        "Angular Developer", "TypeScript Developer",
        "JavaScript Developer", "UI Developer",
        "Web Frontend Engineer",
    ],

    # ── Mobile Development ──
    "mobile": [
        "Mobile Developer", "Mobile Engineer",
        "iOS Developer", "iOS Engineer", "Swift Developer",
        "Android Developer", "Android Engineer", "Kotlin Developer",
        "React Native Developer", "Flutter Developer",
        "Cross-platform Developer",
    ],

    # ── DevOps / SRE / Cloud ──
    "devops": [
        "DevOps Engineer", "Senior DevOps Engineer",
        "Site Reliability Engineer", "SRE",
        "Platform Engineer", "Infrastructure Engineer",
        "Cloud Engineer", "Cloud Architect",
        "AWS Engineer", "Azure Engineer", "GCP Engineer",
        "Kubernetes Engineer", "Docker Engineer",
        "Release Engineer", "Build Engineer",
    ],
    "mlops": [
        "MLOps Engineer", "ML Platform Engineer",
        "DataOps Engineer", "AIOps Engineer",
    ],

    # ── Security ──
    "security": [
        "Security Engineer", "Cybersecurity Engineer",
        "Information Security Engineer", "AppSec Engineer",
        "Application Security Engineer", "Cloud Security Engineer",
        "Security Analyst", "SOC Analyst",
        "Penetration Tester", "Ethical Hacker",
        "DevSecOps Engineer", "Security Architect",
    ],

    # ── QA / Testing ──
    "qa": [
        "QA Engineer", "Quality Assurance Engineer",
        "Test Engineer", "Software Tester",
        "SDET", "Software Development Engineer in Test",
        "Automation Engineer", "Test Automation Engineer",
        "QA Analyst", "Performance Engineer",
        "QA Lead", "Manual Tester",
    ],

    # ── Database / DBA ──
    "database": [
        "Database Administrator", "DBA",
        "Database Engineer", "Database Developer",
        "SQL Developer", "PostgreSQL Developer",
        "MongoDB Developer", "Redis Engineer",
        "Database Reliability Engineer",
    ],

    # ── Systems / Network ──
    "systems": [
        "Systems Engineer", "Systems Administrator",
        "Network Engineer", "Network Administrator",
        "Linux Administrator", "Linux Engineer",
        "Windows Administrator", "IT Administrator",
        "Systems Architect",
    ],

    # ── Embedded / IoT / Hardware ──
    "embedded": [
        "Embedded Engineer", "Embedded Software Engineer",
        "Firmware Engineer", "IoT Engineer",
        "FPGA Engineer", "Hardware Engineer",
        "Embedded Linux Developer", "Robotics Engineer",
    ],

    # ── Game Development ──
    "gamedev": [
        "Game Developer", "Game Programmer",
        "Unity Developer", "Unreal Developer",
        "Game Engine Developer", "Graphics Programmer",
    ],

    # ── Product / Management ──
    "management": [
        "Engineering Manager", "Software Engineering Manager",
        "Technical Lead", "Tech Lead", "Team Lead",
        "VP of Engineering", "Director of Engineering",
        "Head of Engineering", "Head of Data",
        "Data Science Manager", "Data Engineering Manager",
        "CTO", "Chief Technology Officer",
        "Architect", "Solution Architect", "Enterprise Architect",
    ],
    "product": [
        "Product Manager", "Technical Product Manager",
        "Product Owner", "Program Manager",
        "Scrum Master", "Agile Coach",
        "Project Manager IT", "Delivery Manager",
    ],

    # ── Design (UX/UI) ──
    "design": [
        "UX Designer", "UI Designer", "UX/UI Designer",
        "Product Designer", "UX Researcher",
        "Interaction Designer", "Visual Designer",
        "Design Systems Engineer",
    ],

    # ── IT Support / Helpdesk ──
    "support": [
        "IT Support Engineer", "Help Desk Technician",
        "Technical Support Engineer", "IT Specialist",
        "Support Engineer", "Desktop Support",
        "IT Operations Engineer",
    ],
}

# ============================================================================
# RUSSIAN QUERIES (for HH.ru — Russia, Belarus, Kazakhstan, CIS)
# ============================================================================

QUERIES_RU = {
    "data_science": [
        "Data Scientist", "Дата сайентист", "Специалист по данным",
        "Ученый по данным", "ML инженер", "Инженер машинного обучения",
        "AI инженер", "Аналитик данных DS",
    ],
    "machine_learning": [
        "Machine Learning Engineer", "ML Engineer",
        "Инженер машинного обучения", "Разработчик ML",
        "Deep Learning инженер", "AI разработчик",
        "Инженер искусственного интеллекта",
    ],
    "nlp_llm": [
        "NLP инженер", "NLP разработчик",
        "Разработчик чат-ботов", "LLM инженер",
        "Инженер генеративного ИИ",
    ],
    "computer_vision": [
        "Computer Vision инженер", "Разработчик компьютерного зрения",
        "CV инженер", "Инженер обработки изображений",
    ],
    "data_engineering": [
        "Data Engineer", "Дата инженер", "Инженер данных",
        "ETL разработчик", "Big Data инженер",
        "Инженер больших данных", "Spark разработчик",
        "Разработчик хранилищ данных",
    ],
    "analytics": [
        "Аналитик данных", "Data Analyst", "Бизнес-аналитик",
        "Продуктовый аналитик", "Системный аналитик",
        "Маркетинговый аналитик", "Финансовый аналитик",
        "Веб-аналитик", "BI аналитик", "Аналитик BI",
    ],
    "backend": [
        "Python разработчик", "Разработчик Python",
        "Java разработчик", "Разработчик Java",
        "Go разработчик", "Golang разработчик",
        "Backend разработчик", "Бэкенд разработчик",
        "PHP разработчик", "C# разработчик", ".NET разработчик",
        "Node.js разработчик", "Rust разработчик",
        "Kotlin разработчик", "Scala разработчик",
    ],
    "fullstack": [
        "Fullstack разработчик", "Full Stack разработчик",
        "Программист", "Инженер-программист",
        "Разработчик ПО", "Веб-разработчик",
    ],
    "frontend": [
        "Frontend разработчик", "Фронтенд разработчик",
        "React разработчик", "Vue разработчик",
        "Angular разработчик", "JavaScript разработчик",
        "TypeScript разработчик", "Верстальщик",
    ],
    "mobile": [
        "Мобильный разработчик", "iOS разработчик",
        "Android разработчик", "Flutter разработчик",
        "React Native разработчик", "Swift разработчик",
        "Kotlin разработчик мобильных",
    ],
    "devops": [
        "DevOps инженер", "DevOps Engineer",
        "SRE инженер", "Инженер инфраструктуры",
        "Системный администратор Linux",
        "Cloud инженер", "Kubernetes инженер",
        "Инженер платформы",
    ],
    "security": [
        "Инженер информационной безопасности",
        "Специалист по ИБ", "Аналитик SOC",
        "Пентестер", "DevSecOps инженер",
        "Архитектор безопасности",
    ],
    "qa": [
        "QA инженер", "Тестировщик", "QA Engineer",
        "Автоматизатор тестирования", "SDET",
        "Инженер по тестированию", "QA Lead",
        "Ручной тестировщик", "Автоматизатор QA",
    ],
    "database": [
        "Администратор баз данных", "DBA",
        "Разработчик SQL", "PostgreSQL разработчик",
    ],
    "systems": [
        "Системный администратор", "Системный инженер",
        "Сетевой инженер", "Linux администратор",
        "Windows администратор", "IT администратор",
    ],
    "management": [
        "Тимлид", "Техлид", "Team Lead",
        "Tech Lead", "Engineering Manager",
        "Руководитель разработки", "CTO",
        "Технический директор", "Архитектор",
        "Руководитель отдела данных",
    ],
    "product": [
        "Продакт менеджер", "Product Manager",
        "Проджект менеджер", "Project Manager",
        "Владелец продукта", "Scrum Master",
    ],
    "design": [
        "UX дизайнер", "UI дизайнер", "UX/UI дизайнер",
        "Продуктовый дизайнер", "Дизайнер интерфейсов",
    ],
    "support": [
        "IT специалист", "Инженер техподдержки",
        "Специалист техподдержки", "Системный инженер поддержки",
    ],
    "1c": [
        "Разработчик 1С", "Программист 1С",
        "1С разработчик", "Консультант 1С",
    ],
}

# ============================================================================
# FOCUSED / CORE CATEGORIES
# Keep full master list above, but use core categories for regular recurring runs.
# ============================================================================

CORE_CATEGORIES = [
    "data_science",
    "machine_learning",
    "nlp_llm",
    "computer_vision",
    "research",
    "data_engineering",
    "data_architecture",
    "analytics_engineering",
    "analytics",
    "bi",
    "backend",
    "devops",
    "mlops",
]

OPTIONAL_EXTENDED_CATEGORIES = [
    "fullstack",
    "frontend",
    "mobile",
    "security",
    "qa",
    "database",
    "systems",
    "management",
    "product",
]

LOW_PRIORITY_CATEGORIES = [
    "embedded",
    "gamedev",
    "design",
    "support",
    "1c",
]

# ============================================================================
# DERIVED LISTS
# ============================================================================

def _flatten(query_dict: dict[str, list[str]]) -> list[str]:
    """Flatten category dict to a deduplicated list preserving order."""
    seen = set()
    result: list[str] = []
    for queries in query_dict.values():
        for q in queries:
            key = q.lower().strip()
            if key not in seen:
                seen.add(key)
                result.append(q)
    return result


def _subset(query_dict: dict[str, list[str]], categories: list[str]) -> dict[str, list[str]]:
    return {cat: query_dict[cat] for cat in categories if cat in query_dict}


ALL_QUERIES_EN = _flatten(QUERIES_EN)
ALL_QUERIES_RU = _flatten(QUERIES_RU)
ALL_QUERIES_COMBINED = _flatten({**QUERIES_EN, **QUERIES_RU})

CORE_QUERIES_EN = _flatten(_subset(QUERIES_EN, CORE_CATEGORIES))
CORE_QUERIES_RU = _flatten(_subset(QUERIES_RU, CORE_CATEGORIES))

# Category map: query -> category
CATEGORY_MAP: dict[str, str] = {}
for cat, queries in {**QUERIES_EN, **QUERIES_RU}.items():
    for q in queries:
        CATEGORY_MAP.setdefault(q, cat)


def get_queries_for_source(source: str, mode: str | None = None) -> list[str]:
    """
    Get the appropriate query list for a given source.

    mode:
      - "all": full master list (backward-compatible wide coverage)
      - "core": focused subset for regular scheduled runs
    If omitted, reads JOB_QUERY_MODE env var; defaults to "all".
    """
    mode = (mode or os.getenv("JOB_QUERY_MODE", "all")).strip().lower()

    if mode not in {"all", "core"}:
        raise ValueError("mode must be 'all' or 'core'")

    if source == "hh.ru":
        if mode == "core":
            base = CORE_QUERIES_RU + [q for q in CORE_QUERIES_EN if q not in set(CORE_QUERIES_RU)]
        else:
            base = ALL_QUERIES_RU + [q for q in ALL_QUERIES_EN if q not in set(ALL_QUERIES_RU)]
    else:
        base = CORE_QUERIES_EN if mode == "core" else ALL_QUERIES_EN

    return _apply_source_cap(base, source, mode=mode)


def get_core_queries_for_source(source: str) -> list[str]:
    """Convenience wrapper for focused recurring runs."""
    return get_queries_for_source(source, mode="core")


def get_all_queries_for_source(source: str) -> list[str]:
    """Convenience wrapper for exhaustive runs."""
    return get_queries_for_source(source, mode="all")


# Conservative caps to avoid huge noisy runs.
# Core mode is intentionally leaner for recurring scheduled ingestion.
MAX_QUERIES_PER_SOURCE = {
    "all": {
        "hh.ru": 120,
        "adzuna.com": 80,
        "usajobs.gov": 60,
        "himalayas.app": 80,
        "arbeitnow.com": 80,
        "greenhouse.com": 120,
        "lever.co": 120,
        "ashbyhq.com": 120,
    },
    "core": {
        "hh.ru": 80,
        "adzuna.com": 50,
        "usajobs.gov": 40,
        "himalayas.app": 50,
        "arbeitnow.com": 50,
        "greenhouse.com": 90,
        "lever.co": 90,
        "ashbyhq.com": 90,
    },
}


def _apply_source_cap(queries: list[str], source: str, mode: str) -> list[str]:
    cap = MAX_QUERIES_PER_SOURCE.get(mode, {}).get(source)
    if not cap:
        return queries
    return queries[:cap]


# Stats
if __name__ == "__main__":
    print(f"EN queries (all): {len(ALL_QUERIES_EN)}")
    print(f"RU queries (all): {len(ALL_QUERIES_RU)}")
    print(f"Combined unique (all): {len(ALL_QUERIES_COMBINED)}")
    print(f"EN queries (core): {len(CORE_QUERIES_EN)}")
    print(f"RU queries (core): {len(CORE_QUERIES_RU)}")
    print(f"Categories: {len(set(CATEGORY_MAP.values()))}")
    for cat in sorted(set(CATEGORY_MAP.values())):
        count = sum(1 for v in CATEGORY_MAP.values() if v == cat)
        print(f"  {cat}: {count} queries")