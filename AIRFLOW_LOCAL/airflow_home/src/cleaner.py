import json
import pandas as pd
import re

# Функция для перевода звезд в числовой рейтинг
def stars_to_rating(stars: str) -> float:
    star_count = len(stars.strip())  # Количество звезд
    return star_count  # Возвращаем число звезд как рейтинг

# Загрузка данных из JSON
def load_json(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)

# Преобразование данных из JSON в DataFrame
def process_data(file_path, output_path):
    data = load_json(file_path)
    
    movies = []
    for entry in data:
        tooltip = entry.get("tooltip")
        
        # Извлекаем название, год и рейтинг с помощью регулярных выражений
        match = re.match(r"^(.*?)\s\((\d{4})\)\s(★+)$", tooltip)
        if match:
            title = match.group(1)
            year = match.group(2)
            stars = match.group(3)
            
            # Преобразуем звездный рейтинг в числовое значение
            rating = stars_to_rating(stars)
            
            # Добавляем в список
            movies.append({"Title": title, "Year": year, "Rating": rating})

    # Создаем DataFrame из списка
    df = pd.DataFrame(movies)
    
    # Удаляем дубликаты
    df = df.drop_duplicates()

    # Сохраняем в CSV
    df.to_csv(output_path, index=False, encoding='utf-8')

# Параметры файлов
input_file = 'letterboxd_movie_data.json'  # Замените на путь к вашему JSON файлу
output_file = 'movies.csv'  # Путь для сохранения CSV файла

# Запуск обработки данных
process_data(input_file, output_file)

print(f"CSV файл успешно сохранен в: {output_file}")