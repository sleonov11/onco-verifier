import os
import requests
from bs4 import BeautifulSoup
import fitz  # PyMuPDF
import json
from urllib.parse import urljoin
import re
import time

def download_with_retry(url, download_folder, max_retries=2):
    """Скачивает файл с повторными попытками"""
    filename = url.split('/')[-1]
    filepath = os.path.join(download_folder, filename)
    
    # Если файл уже скачан - пропускаем
    if os.path.exists(filepath):
        print(f"  Файл уже есть: {filename}")
        return filepath
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/pdf',
        'Referer': 'https://rosoncoweb.ru/'
    }
    
    for attempt in range(max_retries):
        try:
            print(f"  Попытка {attempt + 1}: {filename}")
            response = requests.get(url, headers=headers, stream=True, timeout=30)
            
            if response.status_code == 200:
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                print(f"  ✓ Скачано: {filename}")
                return filepath
            elif response.status_code == 403:
                print(f"  ✗ Доступ запрещен (403). Сайт блокирует ботов.")
                print(f"    Скачайте файл вручную: {url}")
                return None
            else:
                print(f"  ✗ Ошибка {response.status_code}")
                
        except Exception as e:
            print(f"  ✗ Ошибка: {e}")
        
        time.sleep(2)  # Ждем перед повторной попыткой
    
    return None

def get_pdf_links_from_russco(base_url):
    """Получает список PDF с сайта"""
    print(f"Получаю список PDF с {base_url}...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(base_url, headers=headers, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        pdf_links = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.endswith('.pdf'):
                full_url = urljoin(base_url, href)
                pdf_links.append(full_url)
        
        pdf_links = sorted(list(set(pdf_links)))
        print(f"Найдено {len(pdf_links)} PDF-файлов.")
        return pdf_links
    except Exception as e:
        print(f"Ошибка при получении списка PDF: {e}")
        return []

def process_local_pdfs(download_folder="russco_pdfs"):
    """Обрабатывает уже скачанные PDF из папки"""
    if not os.path.exists(download_folder):
        print(f"Папка {download_folder} не найдена!")
        return []
    
    pdf_files = []
    for file in os.listdir(download_folder):
        if file.endswith('.pdf'):
            pdf_files.append(os.path.join(download_folder, file))
    
    print(f"Найдено локальных PDF: {len(pdf_files)}")
    return pdf_files

def smart_chunk_pdf(pdf_path):
    """Разбивает PDF на чанки"""
    print(f"Обрабатываю: {os.path.basename(pdf_path)}")
    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        print(f"  ✗ Ошибка открытия PDF: {e}")
        return []
    
    chunks = []
    base_name = os.path.basename(pdf_path).replace('.pdf', '').replace('_', ' ')
    cancer_type = base_name[:100]
    
    current_chunk = ""
    current_heading = "Введение"
    chunk_id = 0
    
    for page_num in range(len(doc)):
        page = doc[page_num]
        text = page.get_text()
        
        # Простая разбивка по страницам, если сложный парсинг не работает
        if text:
            chunk_data = {
                "chunk_id": chunk_id,
                "cancer_type": cancer_type,
                "heading": f"Страница {page_num + 1}",
                "text": text[:2000],  # Ограничиваем размер
                "source": os.path.basename(pdf_path),
                "page": page_num + 1
            }
            chunks.append(chunk_data)
            chunk_id += 1
    
    doc.close()
    print(f"  → Создано {len(chunks)} чанков")
    return chunks

def main():
    # 1. Создаем папку для PDF
    download_folder = "russco_pdfs"
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
    
    # 2. Пробуем скачать (но сайт скорее всего заблокирует)
    print("Пытаюсь скачать PDF с сайта...")
    pdf_urls = get_pdf_links_from_russco("https://rosoncoweb.ru/standarts/RUSSCO/")
    
    downloaded_files = []
    if pdf_urls:
        for url in pdf_urls[:5]:  # Пробуем первые 5
            result = download_with_retry(url, download_folder)
            if result:
                downloaded_files.append(result)
    
    # 3. Используем локальные файлы
    print("\n---")
    print("Использую локальные PDF файлы...")
    local_files = process_local_pdfs(download_folder)
    
    if not local_files:
        print("Нет PDF для обработки!")
        print("Скачайте файлы вручную с сайта и положите в папку 'russco_pdfs'")
        return
    
    # 4. Обрабатываем все PDF
    all_chunks = []
    for pdf_file in local_files[:10]:  # Обрабатываем первые 10 файлов
        chunks = smart_chunk_pdf(pdf_file)
        all_chunks.extend(chunks)
    
    # 5. Сохраняем результат
    output = {
        "total_chunks": len(all_chunks),
        "source": "RUSSCO Clinical Recommendations",
        "chunks": all_chunks
    }
    
    with open("russco_chunks.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print(f"\nГотово! Создано {len(all_chunks)} чанков.")
    print("Результат сохранен в 'russco_chunks.json'")

if __name__ == "__main__":
    main()