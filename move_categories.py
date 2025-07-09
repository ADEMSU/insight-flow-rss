import json

# Файлы
source_file = 'rss_urls.json'
target_file = 'rss_sources.json'

# Категории для переноса
categories_to_move = {'technology', 'business'}

# Загружаем исходный файл
with open(source_file, 'r', encoding='utf-8') as f:
    data = json.load(f)

# Делим на два списка: то, что переносим, и то, что остаётся
to_move = [item for item in data['sources'] if item['category'] in categories_to_move]
remaining = [item for item in data['sources'] if item['category'] not in categories_to_move]

# Обновляем исходный файл
with open(source_file, 'w', encoding='utf-8') as f:
    json.dump({'sources': remaining}, f, indent=2, ensure_ascii=False)

# Загружаем/создаём целевой файл
try:
    with open(target_file, 'r', encoding='utf-8') as f:
        existing_data = json.load(f)
        existing_sources = existing_data.get('sources', [])
except FileNotFoundError:
    existing_sources = []

# Добавляем новые записи и сохраняем
updated_sources = existing_sources + to_move
with open(target_file, 'w', encoding='utf-8') as f:
    json.dump({'sources': updated_sources}, f, indent=2, ensure_ascii=False)

print(f"Перенесено: {len(to_move)} источников.")
