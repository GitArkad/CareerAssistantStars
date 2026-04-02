import json
import re


def safe_json_parse(text: str):
    try:
        return json.loads(text)
    except:
        match = re.search(r"\{.*\}", text, re.DOTALL)
        if match:
            return json.loads(match.group())
    return {}


def merge_candidate(old, new):
    if not old:
        return new

    for key, value in new.items():
        if value not in [None, "", [], {}]:
            old[key] = value

    return old