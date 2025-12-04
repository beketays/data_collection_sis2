import asyncio
from playwright.async_api import async_playwright
import json

async def scrape_letterboxd():
    async with async_playwright() as p:
        # Запуск браузера
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        # Список страниц для обхода
        pages = [
            'https://letterboxd.com/dave/list/official-top-250-narrative-feature-films/',
            'https://letterboxd.com/dave/list/official-top-250-narrative-feature-films/page/2/',
            'https://letterboxd.com/dave/list/official-top-250-narrative-feature-films/page/3/'
        ]

        all_movie_data = []  # Список для хранения данных о фильмах

        for url in pages:
            # Открываем страницу
            await page.goto(url)
            print(f"Page loaded: {url}")
            await page.wait_for_selector('li.posteritem.numbered-list-item a.frame')

            # Получаем все элементы, на которые можно навести мышь (фильмы)
            movie_elements = await page.query_selector_all('li.posteritem.numbered-list-item a.frame')

            for movie in movie_elements:
                # Наводим курсор на элемент фильма, чтобы появилась подсказка
                await movie.hover()
                
                # Ожидаем появления подсказки
                await page.wait_for_selector('.twipsy-inner')  # Селектор для подсказки (можно уточнить, если нужно)

                # Извлекаем текст подсказки
                tooltip_text = await page.inner_text('.twipsy-inner')
                
                # Добавляем строку в JSON
                movie_data = {
                    "tooltip": tooltip_text.strip(),  # Собираем полную строку текста
                }

                all_movie_data.append(movie_data)

        # Сохраняем все собранные данные в JSON файл
        with open("letterboxd_movie_data.json", "w", encoding="utf-8") as f:
            json.dump(all_movie_data, f, ensure_ascii=False, indent=2)

        print("Done, collected:", len(all_movie_data))
        await browser.close()

# Запуск асинхронного парсера
asyncio.run(scrape_letterboxd())