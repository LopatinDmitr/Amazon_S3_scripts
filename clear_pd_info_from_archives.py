import sys
import boto3
import zipfile
import pydicom
import os
import time
import shutil

# Получаем список параметров командной строки
args = sys.argv
url = sys.argv[1]

bucket = url.split("/")[-2]
file = url.split("/")[-1]
upload_file = url.split("/")[-1]

print(file)

# Указываем путь для распаковки архива
extract_dir = 'extracted_files'

# Инициализируем клиент Amazon S3
session = boto3.session.Session()

s3 = session.client(
    service_name='s3',
    aws_access_key_id='',
    aws_secret_access_key='',
    endpoint_url='',
)

# Скачиваем zip архив из S3
s3.download_file(bucket, file, 'downloaded_archive.zip')
shutil.copy('downloaded_archive.zip', file + "_backup")

s3.delete_object(Bucket=bucket, Key=file)

# Распаковываем скаченный архив
with zipfile.ZipFile('downloaded_archive.zip', 'r') as zip_ref:
    zip_ref.extractall(extract_dir)

# Проходим по всем файлам в директории
for filename in os.listdir(extract_dir):
    filepath = os.path.join(extract_dir, filename)
    ds = pydicom.dcmread(filepath)

    if ds.Modality == "SR":
        print(f"Найден файл с модальностью SR: {filename}")
        os.remove(os.path.join(extract_dir, filename))
    
    if 'SeriesDescription' in ds and ds.SeriesDescription == "Exam Summary":
        print(f"Найден DICOM-файл с описанием серии 'Exam Summary': {filename}")
        os.remove(os.path.join(extract_dir, filename))

    if 'SeriesDescription' in ds and ds.SeriesDescription == "Dose Report":
        print(f"Найден DICOM-файл с описанием серии 'Dose Report': {filename}")
        os.remove(os.path.join(extract_dir, filename))


# Создаем новый zip архив с оставшимися файлами
with zipfile.ZipFile(file, 'w') as zip_ref:
    for file in os.listdir(extract_dir):
        zip_ref.write(os.path.join(extract_dir, file), file)

time.sleep(3)
print(upload_file)

# Выгружаем новый архив на S3
print(s3.upload_file(upload_file, bucket, upload_file, ExtraArgs={'ACL': 'public-read'}))


# Очищаем временные файлы
os.remove('downloaded_archive.zip')
os.remove(upload_file)
            
# Удаление всех файлов в папке
for file_name in os.listdir(extract_dir):
    file_path = os.path.join(extract_dir, file_name)
    try:
        if os.path.isfile(file_path):
            os.remove(file_path)
    except Exception as e:
        print(f"Ошибка при удалении файла {file_path}: {e}")

# Удаление самой папки
try:
    os.rmdir(extract_dir)
except Exception as e:
    print(f"Ошибка при удалении папки {extract_dir}: {e}")
# break
