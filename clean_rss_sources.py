import json
import requests
import xml.etree.ElementTree as ET
from tqdm import tqdm

def check_rss_source(url: str, timeout: int = 15) -> str:
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        items = root.findall(".//item")
        return "ok" if items else "empty"
    except requests.exceptions.Timeout:
        return "timeout"
    except Exception:
        return "error"

def clean_rss_sources(file_path: str):
    print(f"[DEBUG] Загружаем файл: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        sources = data.get("sources", [])
    print(f"[DEBUG] Найдено источников: {len(sources)}")

    cleaned_sources = []
    for source in tqdm(sources, desc="Проверка источников"):
        url = source.get("url") or source.get("rss_url")
        if not url:
            continue
        status = check_rss_source(url)
        source["status"] = status
        if status not in ("timeout", "error"):
            cleaned_sources.append(source)

    print(f"[INFO] Очищено: осталось {len(cleaned_sources)} источников из {len(sources)}")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump({"sources": cleaned_sources}, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    clean_rss_sources("rss_sources.json")
