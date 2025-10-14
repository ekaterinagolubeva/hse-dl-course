import pandas as pd
import requests
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

DEFAULT_SAVE_FOLDER = os.path.join(os.getcwd(), "NGA_Dataset")
os.makedirs(DEFAULT_SAVE_FOLDER, exist_ok=True)
log_file = os.path.join(DEFAULT_SAVE_FOLDER, "download_log.txt")
description_file = os.path.join(DEFAULT_SAVE_FOLDER, "description.csv")
log_lock = Lock()  # To prevent race conditions in logging
description_lock = Lock()  # To prevent race conditions in description writing

# Logging function (thread-safe)
def write_log(message):
    with log_lock:
        with open(log_file, "a") as log:
            log.write(message + "\n")

# Description writing function (thread-safe)
def write_description(description_data):
    """
    Записывает описание изображения в CSV файл.
    description_data: словарь с полями uuid, filename, artist, title, date, classification, medium
    """
    with description_lock:
        # Проверяем, существует ли файл
        file_exists = os.path.exists(description_file)
        
        # Создаём DataFrame из одной записи
        df = pd.DataFrame([description_data])
        
        # Записываем в файл (добавляем заголовок только если файл новый)
        df.to_csv(description_file, mode='a', header=not file_exists, index=False)

# Download function (run in parallel)
def download_image(row):
    base_url = row["iiifurl"]
    uuid = row["uuid"]
    image_url = f"{base_url}/full/full/360/default.jpg" # few images are partially returned if 360 is replaced with a zero
    file_name = f"{uuid}.jpg"
    file_path = os.path.join(DEFAULT_SAVE_FOLDER, file_name)

    try:
        response = requests.get(image_url, stream=True, timeout=15)

        if response.status_code == 200:
            with open(file_path, "wb") as file:
                for chunk in response.iter_content(1024):
                    if chunk:
                        file.write(chunk)
            print(f"Downloaded: {file_name}")
            write_log(f"SUCCESS: {file_name} | URL: {image_url}")
            
            # Сохраняем описание картины
            description_data = {
                'uuid': uuid,
                'filename': file_name,
                'artist': row.get('artist', 'Unknown'),
                'title': row.get('title', 'Untitled'),
                'date': row.get('displaydate', 'Unknown'),
                'classification': row.get('classification', 'Unknown'),
                'medium': row.get('medium', 'Unknown')
            }
            write_description(description_data)
            
        else:
            print(f"FAILED (HTTP {response.status_code}): {file_name}")
            write_log(f"FAILURE (HTTP {response.status_code}): {file_name} | URL: {image_url}")

    except Exception as e:
        print(f"ERROR: {file_name} | {e}")
        write_log(f"ERROR: {file_name} | URL: {image_url} | Exception: {e}")

def filter_by_classification(images_df, objects_path, allowed_classifications=None, 
                            excluded_subclassifications=None):
    """
    Фильтрует изображения по классификации объектов (исключает наброски и другие типы).
    
    Args:
        images_df: DataFrame с изображениями (published_images.csv)
        objects_path: путь к файлу objects.csv
        allowed_classifications: список разрешённых классификаций (None = все разрешены)
        excluded_subclassifications: список исключённых подклассификаций (например, наброски)
    
    Returns:
        Отфильтрованный DataFrame с изображениями
    """
    # Загружаем данные об объектах
    print("\nЗагрузка данных об объектах...")
    objects = pd.read_csv(objects_path, low_memory=False)
    
    # Фильтруем по классификации
    if allowed_classifications:
        print(f"\nРазрешённые классификации: {', '.join(allowed_classifications)}")
        objects = objects[objects['classification'].isin(allowed_classifications)]
        print(f"Объектов после фильтрации по классификации: {len(objects)}")
    
    # Исключаем нежелательные подклассификации
    if excluded_subclassifications:
        print(f"\nИсключаемые подклассификации: {', '.join(excluded_subclassifications)}")
        before_count = len(objects)
        objects = objects[~objects['subclassification'].isin(excluded_subclassifications)]
        print(f"Исключено объектов: {before_count - len(objects)}")
        print(f"Объектов после фильтрации: {len(objects)}")
    
    # Получаем ID объектов
    object_ids = objects['objectid'].unique()
    
    # Фильтруем изображения по объектам
    filtered_images = images_df[images_df['depictstmsobjectid'].isin(object_ids)]
    print(f"\nНайдено изображений после фильтрации: {len(filtered_images)}")
    
    return filtered_images

def filter_by_artists(images_df, constituents_path, objects_constituents_path, artist_names):
    """
    Фильтрует изображения по списку имён авторов.
    
    Args:
        images_df: DataFrame с изображениями (published_images.csv)
        constituents_path: путь к файлу constituents.csv
        objects_constituents_path: путь к файлу objects_constituents.csv
        artist_names: список имён авторов для фильтрации (можно использовать частичные совпадения)
    
    Returns:
        Отфильтрованный DataFrame с изображениями только указанных авторов
    """
    # Загружаем данные об авторах
    constituents = pd.read_csv(constituents_path)
    objects_constituents = pd.read_csv(objects_constituents_path)
    
    # Фильтруем авторов по именам (поиск по частичному совпадению)
    if artist_names:
        artist_filter = constituents['preferreddisplayname'].str.contains(
            '|'.join(artist_names), case=False, na=False
        )
        selected_constituents = constituents[artist_filter]
        
        print(f"\nНайдено авторов: {len(selected_constituents)}")
        print("Список авторов:")
        for name in selected_constituents['preferreddisplayname'].unique():
            print(f"  - {name}")
        
        # Получаем ID выбранных авторов
        constituent_ids = selected_constituents['constituentid'].tolist()
        
        # Находим объекты этих авторов (только когда они являются создателями)
        artist_objects = objects_constituents[
            (objects_constituents['constituentid'].isin(constituent_ids)) &
            (objects_constituents['roletype'] == 'artist')
        ]
        
        # Получаем ID объектов
        object_ids = artist_objects['objectid'].unique()
        print(f"Найдено объектов: {len(object_ids)}")
        
        # Фильтруем изображения по объектам
        filtered_images = images_df[images_df['depictstmsobjectid'].isin(object_ids)]
        print(f"Найдено изображений: {len(filtered_images)}")
        
        return filtered_images
    else:
        print("Не указаны авторы для фильтрации. Загружаем все изображения.")
        return images_df

def download_dataset(images_path, max_threads=8, artist_names=None, 
                    constituents_path=None, objects_constituents_path=None,
                    objects_path=None, allowed_classifications=None,
                    excluded_subclassifications=None):
    """
    Скачивает изображения из датасета.
    
    Args:
        images_path: путь к published_images.csv
        max_threads: количество потоков для параллельной загрузки
        artist_names: список имён авторов для фильтрации (опционально)
        constituents_path: путь к constituents.csv (опционально, нужен для фильтрации)
        objects_constituents_path: путь к objects_constituents.csv (опционально, нужен для фильтрации)
        objects_path: путь к objects.csv (опционально, нужен для фильтрации по классификации)
        allowed_classifications: список разрешённых классификаций (опционально)
        excluded_subclassifications: список исключённых подклассификаций (опционально)
    """
    # Удаляем старый description.csv перед началом новой сессии
    if os.path.exists(description_file):
        os.remove(description_file)
        print(f"Удалён старый файл {description_file}")
    
    data = pd.read_csv(images_path)
    data = data.dropna(subset=["iiifurl"])
    
    # Фильтрация по классификации, если указаны параметры
    if objects_path and (allowed_classifications or excluded_subclassifications):
        data = filter_by_classification(data, objects_path, allowed_classifications, 
                                       excluded_subclassifications)
        if len(data) == 0:
            print("Не найдено изображений после фильтрации по классификации!")
            return
    
    # Фильтрация по авторам, если указаны
    if artist_names and constituents_path and objects_constituents_path:
        data = filter_by_artists(data, constituents_path, objects_constituents_path, artist_names)
        if len(data) == 0:
            print("Не найдено изображений для указанных авторов!")
            return
    
    # Добавляем информацию об объектах к данным изображений
    if objects_path:
        print("\nДобавление метаданных объектов...")
        objects = pd.read_csv(objects_path, low_memory=False)
        # Объединяем данные изображений с данными объектов
        data = data.merge(
            objects[['objectid', 'title', 'displaydate', 'classification', 'medium']], 
            left_on='depictstmsobjectid', 
            right_on='objectid', 
            how='left'
        )
    
    # Добавляем информацию об авторах
    if constituents_path and objects_constituents_path:
        print("Добавление информации об авторах...")
        constituents = pd.read_csv(constituents_path)
        objects_constituents = pd.read_csv(objects_constituents_path)
        
        # Получаем авторов для каждого объекта
        artists = objects_constituents[objects_constituents['roletype'] == 'artist'].merge(
            constituents[['constituentid', 'preferreddisplayname']], 
            on='constituentid'
        )
        
        # Группируем по objectid и объединяем имена авторов
        artists_grouped = artists.groupby('objectid')['preferreddisplayname'].apply(
            lambda x: ', '.join(x)
        ).reset_index()
        artists_grouped.columns = ['objectid', 'artist']
        
        # Добавляем к данным
        data = data.merge(artists_grouped, left_on='depictstmsobjectid', right_on='objectid', how='left')

    write_log("=" * 60)
    write_log("New Download Session Started")
    if artist_names:
        write_log(f"Filtering by artists: {', '.join(artist_names)}")
    if allowed_classifications:
        write_log(f"Allowed classifications: {', '.join(allowed_classifications)}")
    if excluded_subclassifications:
        write_log(f"Excluded subclassifications: {', '.join(excluded_subclassifications)}")
    write_log("=" * 60)

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        futures = [executor.submit(download_image, row) for _, row in data.iterrows()]
        for future in as_completed(futures):
            pass  # We don't need to gather results, everything is logged inside threads

    print("Download complete!")
    print(f"\nОписания сохранены в {description_file}")

if __name__ == "__main__":
    print("=" * 60)
    print("NGA Image Downloader")
    print("=" * 60)
    
    images_path = "./3rd_party/opendata/data/published_images.csv"
    objects_path = "./3rd_party/opendata/data/objects.csv"
    constituents_path = "./3rd_party/opendata/data/constituents.csv"
    objects_constituents_path = "./3rd_party/opendata/data/objects_constituents.csv"
    
    # Фильтрация по классификации
    print("\n" + "=" * 60)
    print("ФИЛЬТРАЦИЯ ПО ТИПУ ПРОИЗВЕДЕНИЙ")
    print("=" * 60)
    
    filter_classification = input("\nФильтровать по классификации (тип произведения)? (y/n): ").strip().lower()
    
    allowed_classifications = None
    excluded_subclassifications = None
    
    if filter_classification == 'y':
        print("\nДоступные классификации:")
        print("  1. Painting (Живопись)")
        print("  2. Sculpture (Скульптура)")
        print("  3. Print (Гравюры)")
        print("  4. Drawing (Рисунки)")
        print("  5. Photograph (Фотографии)")
        print("  6. Decorative Art (Декоративное искусство)")
        print("  7. Все (без фильтрации)")
        
        class_choice = input("\nВыберите номера через запятую (например: 1,2,6) или 7 для всех: ").strip()
        
        classification_map = {
            '1': 'Painting',
            '2': 'Sculpture',
            '3': 'Print',
            '4': 'Drawing',
            '5': 'Photograph',
            '6': 'Decorative Art'
        }
        
        if class_choice != '7':
            selected_nums = [n.strip() for n in class_choice.split(',')]
            allowed_classifications = [classification_map[n] for n in selected_nums if n in classification_map]
        
        # Исключение набросков и эскизов
        exclude_sketches = input("\nИсключить наброски, эскизы и технические материалы? (y/n): ").strip().lower()
        
        if exclude_sketches == 'y':
            excluded_subclassifications = [
                'Drawing',  # Рисунки/наброски
                'Work Print',  # Рабочие отпечатки
                'Contact Sheet',  # Контактные листы
                'Archival',  # Архивные материалы
                'Printmaking Matrices',  # Матрицы для печати
            ]
            print(f"Исключаются: {', '.join(excluded_subclassifications)}")
    
    # Фильтрация по авторам
    print("\n" + "=" * 60)
    print("ФИЛЬТРАЦИЯ ПО АВТОРАМ")
    print("=" * 60)
    
    filter_artists = input("\nФильтровать по авторам? (y/n): ").strip().lower()
    
    artist_names = None
    if filter_artists == 'y':
        print("\nВведите имена авторов (через запятую).")
        print("Можно использовать частичные совпадения, например 'Monet' или 'Vincent':")
        artist_input = input("> ").strip()
        artist_names = [name.strip() for name in artist_input.split(',') if name.strip()]
    
    # Запуск скачивания
    print("\n" + "=" * 60)
    print("НАЧАЛО СКАЧИВАНИЯ")
    print("=" * 60)
    
    download_dataset(
        images_path, 
        max_threads=8,
        artist_names=artist_names,
        constituents_path=constituents_path,
        objects_constituents_path=objects_constituents_path,
        objects_path=objects_path,
        allowed_classifications=allowed_classifications,
        excluded_subclassifications=excluded_subclassifications
    )