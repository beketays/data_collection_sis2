import sqlite3
import pandas as pd

# Функция для чтения CSV и создания таблицы в SQLite
def create_sqlite_table(csv_file, db_file):
    # Чтение данных из CSV в DataFrame
    df = pd.read_csv(csv_file)

    # Создание подключения к базе данных SQLite (если файл не существует, он будет создан)
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Создание таблицы (если таблица не существует)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS movies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        Title TEXT,
        Year INTEGER,
        Rating INTEGER
    )
    ''')

    # Вставка данных из DataFrame в таблицу SQLite
    for _, row in df.iterrows():
        cursor.execute('''
        INSERT INTO movies (Title, Year, Rating)
        VALUES (?, ?, ?)
        ''', (row['Title'], row['Year'], row['Rating']))

    # Сохранение изменений и закрытие соединения
    conn.commit()
    conn.close()

    print(f"Таблица успешно создана в базе данных: {db_file}")

# Параметры файлов
csv_file = 'movies.csv'  # Путь к вашему CSV файлу
db_file = 'movies.db'    # Путь для сохранения SQLite базы данных

# Запуск скрипта
create_sqlite_table(csv_file, db_file)