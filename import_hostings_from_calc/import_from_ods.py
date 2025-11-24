import os
import pandas as pd
import psycopg2
import ezodf
import re

# Читаем ODS файл с сохранением гиперссылок
def read_ods_with_hyperlinks(file_path):
    doc = ezodf.opendoc(file_path)
    sheet = doc.sheets[0]  # первый лист
    
    # Получаем заголовки
    headers = []
    for cell in sheet.row(0):
        headers.append(cell.value)
    
    # Собираем данные
    rows = []
    for row_idx in range(1, sheet.nrows()):
        row_data = []
        for col_idx, cell in enumerate(sheet.row(row_idx)):
            # Проверяем есть ли гиперссылка
            if cell.hyperlink:
                # Сохраняем и текст и ссылку
                row_data.append({
                    'text': cell.value,
                    'url': cell.hyperlink
                })
            else:
                # Просто значение ячейки
                row_data.append(cell.value)
        rows.append(row_data)
    
    # Создаем DataFrame
    df = pd.DataFrame(rows, columns=headers)
    return df

# Альтернативный способ с odfpy
def read_ods_with_odfpy(file_path):
    from odf import text, teletype
    from odf.opendocument import load
    
    doc = load(file_path)
    all_hyperlinks = []
    
    # Ищем все гиперссылки в документе
    for link in doc.getElementsByType(text.A):
        href = link.getAttribute('href')
        link_text = teletype.extractText(link)
        all_hyperlinks.append({'text': link_text, 'url': href})
    
    print("Найденные гиперссылки в документе:")
    for link in all_hyperlinks[:10]:
        print(f"  '{link['text']}' -> {link['url']}")
    
    return all_hyperlinks

try:
    print("Попытка чтения ODS с ezodf...")
    df = read_ods_with_hyperlinks('hostings.ods')
except Exception as e:
    print(f"Ошибка при чтении с ezodf: {e}")
    print("Попытка чтения с pandas...")
    # Резервный вариант с pandas
    df = pd.read_excel('hostings.ods', engine='odf')

print("Первые строки из ODS файла:")
print(df.head(10))
print("\nСтолбцы:")
print(df.columns.tolist())

# Функция для обработки гиперссылок
def parse_hyperlink(cell):
    if cell is None:
        return "", None
    
    # Если это словарь с гиперссылкой
    if isinstance(cell, dict) and 'text' in cell and 'url' in cell:
        return cell['text'], cell['url']
    
    # Если это строка
    cell_str = str(cell)
    
    # Telegram бот
    if cell_str.startswith('@'):
        username = cell_str
        url = f"https://t.me/{username[1:]}"
        return username, url
    
    # Проверяем, есть ли URL в тексте (ручной парсинг)
    url_pattern = r'(https?://[^\s]+)'
    url_match = re.search(url_pattern, cell_str)
    if url_match:
        url = url_match.group(1)
        name = re.sub(url_pattern, '', cell_str).strip()
        return name, url
    
    # Просто текст - проверяем, похоже ли на домен
    if re.match(r'^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', cell_str):
        return cell_str, f"https://{cell_str}"
    
    # Просто текст без ссылки
    return cell_str, None

# Функция для обработки цены
def parse_price(price_cell):
    if price_cell is None:
        return None
    
    price_str = str(price_cell)
    
    # Удаляем символ $ и пробелы, преобразуем в число
    price_str = price_str.replace('$', '').replace(' ', '').strip()
    
    try:
        return float(price_str) if price_str else None
    except (ValueError, TypeError):
        return None

# Диагностика формата данных
print("\nДиагностика формата данных в столбце 'Хостинг':")
for i, cell in enumerate(df['Хостинг'].head(10)):
    print(f"{i}: {repr(cell)} (тип: {type(cell)})")

# Обрабатываем гиперссылки
print("\nОбработка гиперссылок...")
df[['hosting_name', 'url']] = df['Хостинг'].apply(
    lambda x: pd.Series(parse_hyperlink(x))
)

# Обрабатываем остальные столбцы
df['min_price_in_dollars'] = df['Минимальная цена'].apply(parse_price)

# Переименовываем столбцы
column_mapping = {
    'Статус': 'status',
    'Значение риска proxycheck.io (APIv3)': 'risk',
    'Преимущества': 'advantages', 
    'Недостатки': 'disadvantages',
    'Расположение хостинга': 'hosting_location',
    'Расположение серверов': 'servers_location'
}

for old_col, new_col in column_mapping.items():
    if old_col in df.columns:
        df[new_col] = df[old_col]
    else:
        df[new_col] = None

# Заменяем NaN на None
df = df.where(pd.notnull(df), None)

# Выводим статистику
print(f"\nСтатистика обработки:")
print(f"Всего записей: {len(df)}")
print(f"С ссылками: {df['url'].notna().sum()}")
print(f"Без ссылок: {df['url'].isna().sum()}")
print(f"Telegram ботов: {df['hosting_name'].str.startswith('@').sum()}")

print("\nПримеры обработанных данных:")
for i, row in df[['hosting_name', 'url', 'min_price_in_dollars']].head(15).iterrows():
    print(f"{i}: name='{row['hosting_name']}', url={row['url']}")

# Покажем записи, где есть ссылки
print("\nЗаписи со ссылками:")
has_links = df[df['url'].notna()]
for i, row in has_links[['hosting_name', 'url']].head(10).iterrows():
    print(f"  {row['hosting_name']} -> {row['url']}")

# Подключаемся к PostgreSQL
db_host = os.getenv('BLOG_DB_HOST')
db_name = os.getenv('BLOG_DB_NAME')
db_user = os.getenv('BLOG_DB_USER')
db_password = os.getenv('BLOG_DB_PASSWORD')

try:
    conn = psycopg2.connect(
        dbname=db_name, 
        user=db_user, 
        password=db_password, 
        host=db_host
    )
    cur = conn.cursor()

    # Вставляем данные
    inserted_count = 0
    for _, row in df.iterrows():
        try:
            cur.execute("""
                INSERT INTO hosting (
                    hosting_name, url, status, risk, advantages, 
                    disadvantages, hosting_location, servers_location, min_price_in_dollars
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING hosting_id
            """, (
                row['hosting_name'][:500] if row['hosting_name'] else None,  # обрезаем длинные имена
                row['url'][:1000] if row['url'] else None,  # обрезаем длинные URL
                str(row.get('status'))[:500] if row.get('status') else None,
                int(row.get('risk')) if pd.notna(row.get('risk')) and str(row.get('risk')).isdigit() else None,
                str(row.get('advantages'))[:2000] if row.get('advantages') else None,
                str(row.get('disadvantages'))[:2000] if row.get('disadvantages') else None,
                str(row.get('hosting_location'))[:500] if row.get('hosting_location') else None,
                str(row.get('servers_location'))[:500] if row.get('servers_location') else None,
                row.get('min_price_in_dollars')
            ))
            
            hosting_id = cur.fetchone()[0]
            inserted_count += 1
            
            # Присваиваем категории
            categories_to_assign = [2]  # Настройте по необходимости
            for cat_id in categories_to_assign:
                try:
                    cur.execute(
                        "INSERT INTO hosting_category (hosting_id, category_id) VALUES (%s, %s)",
                        (hosting_id, cat_id)
                    )
                except Exception as e:
                    print(f"Ошибка при добавлении категории {cat_id} для hosting_id {hosting_id}: {e}")
                    
        except Exception as e:
            print(f"Ошибка при вставке записи {row['hosting_name']}: {e}")
            continue

    conn.commit()
    print(f"\nИмпорт завершён успешно! Вставлено записей: {inserted_count}/{len(df)}")

except Exception as e:
    print(f"Ошибка при работе с базой данных: {e}")
    if conn:
        conn.rollback()
finally:
    if cur:
        cur.close()
    if conn:
        conn.close()