import os
import pandas as pd
import psycopg2
import re

# CSV из Calc
df = pd.read_csv('hostings.csv')  # столбец с гиперссылками называется 'Link'

print(df.head(10))

# Разделяем HYPERLINK на name и url
def split_hyperlink(cell):
    match = re.match(r'=HYPERLINK\("([^"]+)"\s*;\s*"([^"]+)"\)', str(cell))
    if match:
        url, name = match.groups()
        return pd.Series([name, url])
    return pd.Series([cell, ""])

df[['name', 'url']] = df['Хостинг'].apply(split_hyperlink)
df.drop(columns=['Хостинг'], inplace=True)

db_host = os.getenv('BLOG_DB_HOST')
db_name = os.getenv('BLOG_DB_NAME')
db_user = os.getenv('BLOG_DB_USER')
db_password = os.getenv('BLOG_DB_PASSWORD')

# Подключаемся к PostgreSQL
conn = psycopg2.connect(f"dbname={db_name} user={db_user} password={db_password} host={db_host}")
cur = conn.cursor()

# Пример категории для всех (можно изменить per row)
categories_mapping = {
    'Избранное': 1,
    'За крипту': 2,
    'Бесплатные': 3,
    'Списки': 4
}

for _, row in df.iterrows():
    cur.execute("INSERT INTO hosting (hosting_name, url) VALUES (%s, %s) RETURNING hosting_id", (row['name'], row['url']))
    hosting_id = cur.fetchone()[0]
    # Присвоение категорий (можно тут менять на разные per row)
    for cat_id in [2]:  # например Избранное и Бесплатные
        cur.execute("INSERT INTO hosting_category (hosting_id, category_id) VALUES (%s, %s)", (hosting_id, cat_id))

conn.commit()
cur.close()
conn.close()
print("Импорт завершён")
