"""
Вспомогательный скрипт для поиска авторов в датасете NGA
"""

import pandas as pd
import os

DATA_DIR = "./3rd_party/opendata/data"
CONSTITUENTS_PATH = os.path.join(DATA_DIR, "constituents.csv")
OBJECTS_CONSTITUENTS_PATH = os.path.join(DATA_DIR, "objects_constituents.csv")

def search_artists(search_term="", show_stats=True):
    """
    Поиск авторов в датасете
    
    Args:
        search_term: строка для поиска (если пусто, показать всех)
        show_stats: показывать статистику по количеству работ
    """
    # Загружаем данные
    constituents = pd.read_csv(CONSTITUENTS_PATH)
    
    # Фильтруем только художников
    artists = constituents[constituents['artistofngaobject'] == 1].copy()
    
    # Поиск по имени
    if search_term:
        mask = artists['preferreddisplayname'].str.contains(
            search_term, case=False, na=False
        )
        artists = artists[mask]
    
    if len(artists) == 0:
        print(f"Авторы с '{search_term}' не найдены.")
        return
    
    # Если нужна статистика
    if show_stats:
        objects_constituents = pd.read_csv(OBJECTS_CONSTITUENTS_PATH)
        
        # Подсчитываем количество работ для каждого автора
        artist_works = objects_constituents[
            objects_constituents['roletype'] == 'artist'
        ].groupby('constituentid').size().reset_index(name='works_count')
        
        # Объединяем с информацией об авторах
        artists = artists.merge(
            artist_works, 
            left_on='constituentid', 
            right_on='constituentid',
            how='left'
        )
        artists['works_count'] = artists['works_count'].fillna(0).astype(int)
        
        # Сортируем по количеству работ
        artists = artists.sort_values('works_count', ascending=False)
    
    # Выводим результаты
    print(f"\nНайдено авторов: {len(artists)}")
    print("=" * 80)
    
    for _, artist in artists.iterrows():
        name = artist['preferreddisplayname']
        dates = artist['displaydate'] if pd.notna(artist['displaydate']) else "?"
        nationality = artist['nationality'] if pd.notna(artist['nationality']) else "?"
        
        if show_stats:
            works = int(artist['works_count'])
            print(f"{name:<40} | {dates:<20} | {nationality:<15} | Работ: {works}")
        else:
            print(f"{name:<40} | {dates:<20} | {nationality}")
    
    print("=" * 80)

def list_top_artists(top_n=20):
    """
    Показать топ авторов по количеству работ
    
    Args:
        top_n: количество авторов для отображения
    """
    constituents = pd.read_csv(CONSTITUENTS_PATH)
    objects_constituents = pd.read_csv(OBJECTS_CONSTITUENTS_PATH)
    
    # Подсчитываем работы
    artist_works = objects_constituents[
        objects_constituents['roletype'] == 'artist'
    ].groupby('constituentid').size().reset_index(name='works_count')
    
    # Объединяем с именами
    artists = constituents.merge(artist_works, on='constituentid')
    artists = artists.sort_values('works_count', ascending=False).head(top_n)
    
    print(f"\nТоп-{top_n} авторов по количеству работ в коллекции:")
    print("=" * 80)
    
    for i, (_, artist) in enumerate(artists.iterrows(), 1):
        name = artist['preferreddisplayname']
        works = int(artist['works_count'])
        dates = artist['displaydate'] if pd.notna(artist['displaydate']) else "?"
        print(f"{i:2d}. {name:<35} | {dates:<20} | Работ: {works:>4}")
    
    print("=" * 80)

if __name__ == "__main__":
    print("=" * 80)
    print("NGA Artist Search Tool")
    print("=" * 80)
    
    choice = input("\n1. Поиск авторов\n2. Топ авторов по количеству работ\n3. Выход\n\nВыберите опцию: ").strip()
    
    if choice == "1":
        search_term = input("\nВведите имя автора (или оставьте пустым для показа всех): ").strip()
        search_artists(search_term, show_stats=True)
    elif choice == "2":
        try:
            top_n = int(input("\nСколько авторов показать? (по умолчанию 20): ").strip() or "20")
        except:
            top_n = 20
        list_top_artists(top_n)
    else:
        print("Выход.")

