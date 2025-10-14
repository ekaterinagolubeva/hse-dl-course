import pandas as pd
import os

# Пути к данным
images_path = "./3rd_party/opendata/data/published_images.csv"
constituents_path = "./3rd_party/opendata/data/constituents.csv"
objects_constituents_path = "./3rd_party/opendata/data/objects_constituents.csv"
objects_path = "./3rd_party/opendata/data/objects.csv"

# Загрузка данных
print("Загрузка данных...")
images = pd.read_csv(images_path)
constituents = pd.read_csv(constituents_path)
objects_constituents = pd.read_csv(objects_constituents_path)
objects = pd.read_csv(objects_path)

# Получаем список скачанных файлов
dataset_dir = "./NGA_Dataset"
downloaded_files = [f for f in os.listdir(dataset_dir) if f.endswith('.jpg')]
downloaded_uuids = [f.replace('.jpg', '') for f in downloaded_files]

print(f"\nВсего скачано изображений: {len(downloaded_files)}")

# Фильтруем images по скачанным uuid
downloaded_images = images[images['uuid'].isin(downloaded_uuids)]

# Получаем объекты
object_ids = downloaded_images['depictstmsobjectid'].unique()
downloaded_objects = objects[objects['objectid'].isin(object_ids)]

# Получаем авторов
authors_relations = objects_constituents[
    (objects_constituents['objectid'].isin(object_ids)) &
    (objects_constituents['roletype'] == 'artist')
]

# Объединяем с данными об авторах
authors_info = authors_relations.merge(
    constituents[['constituentid', 'preferreddisplayname']], 
    on='constituentid'
)

# Объединяем с объектами для получения полной информации
full_info = authors_info.merge(
    downloaded_objects[['objectid', 'title', 'displaydate']], 
    on='objectid'
)

# Объединяем с изображениями для получения uuid
full_info = full_info.merge(
    downloaded_images[['depictstmsobjectid', 'uuid']], 
    left_on='objectid', 
    right_on='depictstmsobjectid'
)

print("\n" + "="*80)
print("АНАЛИЗ ДАТАСЕТА")
print("="*80)

# Группировка по авторам
by_author = full_info.groupby('preferreddisplayname').size().reset_index(name='count')
print("\nРаспределение по авторам:")
for _, row in by_author.iterrows():
    print(f"  {row['preferreddisplayname']}: {row['count']} изображений")

# Детальная информация по каждому автору
print("\n" + "="*80)
for author in by_author['preferreddisplayname'].unique():
    print(f"\n{author.upper()}")
    print("-"*80)
    author_works = full_info[full_info['preferreddisplayname'] == author]
    for _, work in author_works.iterrows():
        print(f"  • {work['uuid']}.jpg - {work['title']} ({work['displaydate']})")

# Сохраняем информацию в CSV
output_file = "./NGA_Dataset/dataset_info.csv"
full_info[['uuid', 'preferreddisplayname', 'title', 'displaydate']].to_csv(
    output_file, 
    index=False
)
print(f"\n\nИнформация сохранена в {output_file}")

# Создаём файлы со списками для каждого автора
for author in by_author['preferreddisplayname'].unique():
    author_works = full_info[full_info['preferreddisplayname'] == author]
    author_uuids = author_works['uuid'].tolist()
    
    # Безопасное имя файла
    safe_name = author.replace(' ', '_').replace(',', '')
    list_file = f"./NGA_Dataset/{safe_name}_images.txt"
    
    with open(list_file, 'w') as f:
        for uuid in author_uuids:
            f.write(f"{uuid}.jpg\n")
    
    print(f"Список для {author}: {list_file}")

