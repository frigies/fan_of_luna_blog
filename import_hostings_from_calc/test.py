import pandas as pd

# Читаем CSV
df = pd.read_csv('hostings.csv')

print("Диагностика формата гиперссылок:")
print("Столбцы:", df.columns.tolist())
print("\nПервые 5 значений столбца 'Хостинг':")
for i, value in enumerate(df['Хостинг'].head()):
    print(f"{i}: {repr(value)}")
    
print("\nТип данных:", type(df['Хостинг'].iloc[0]))