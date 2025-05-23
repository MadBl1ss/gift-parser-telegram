import os, re, json, time, shutil, requests, argparse, hashlib
import asyncio
import aiohttp
from pathlib import Path
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm as async_tqdm # For async progress bar
from tqdm import tqdm # For sync progress bar if needed

# COLLECTION_SLUG will be set in main() from args
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# Global caches, initialized once
downloaded_models_cache = {}
downloaded_patterns_cache = {}

def normalize_url(u: str | None) -> str | None:
    if not u:
        return None
    u = u.strip()
    if u.startswith('//'):
        return 'https:' + u
    if u.startswith('/file/'):
        return 'https://cdn4.cdn-telegram.org' + u
    return u

def parse_page(html: str, url_id: int, current_collection_slug: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    grad_from = grad_to = pattern_png_url_val = pattern_tint = ""
    tgs_url_val = None
    img_b64 = None

    not_found_element = soup.select_one("div.tgme_page_error_title")
    if not_found_element and "not be found" in not_found_element.get_text(strip=True).lower():
        tqdm.write(f"[{url_id}] NFT для коллекции {current_collection_slug} не найден (сообщение на странице).")
        return None
    
    svg_texts_tags = soup.select("div.tgme_gift_preview > svg text")
    nft_name = ""
    collectible_id_parsed = None

    if svg_texts_tags:
        if len(svg_texts_tags) > 0:
            nft_name_candidate = svg_texts_tags[0].get_text(strip=True)
            nft_name = nft_name_candidate

        if len(svg_texts_tags) > 1:
            match = re.search(r"#(\d+)", svg_texts_tags[1].get_text(strip=True))
            if match:
                try:
                    collectible_id_parsed = int(match.group(1))
                except ValueError:
                    pass
    
    collectible_id = collectible_id_parsed if collectible_id_parsed is not None else url_id

    attrs = {}
    table = soup.find("table", class_="tgme_gift_table")
    if table:
        for row in table.find_all("tr"):
            th, td = row.find("th"), row.find("td")
            if th and td:
                attrs[th.get_text(strip=True).lower()] = td.get_text(" ", strip=True)
    
    if not nft_name:
        meta_title = soup.select_one('meta[property="og:title"]')
        if meta_title and meta_title.has_attr("content"):
            nft_name = meta_title["content"].split("–")[0].strip()

    src_svg = soup.select_one('picture.tgme_gift_model source[type="image/svg+xml"]')
    if src_svg and src_svg.has_attr("srcset"):
        s = src_svg["srcset"]
        if s.startswith("data:image/svg+xml;base64,"):
            img_b64 = s.split(",", 1)[1]

    tgs_src = soup.select_one('source[type="application/x-tgsticker"]')
    tgs_url_val = tgs_src["srcset"] if tgs_src and tgs_src.has_attr("srcset") else None

    svg_element = soup.select_one("div.tgme_gift_preview > svg")
    if svg_element:
        stops = svg_element.find_all("stop")
        if len(stops) >= 2:
            grad_from = stops[0].get("stop-color", "")
            grad_to = stops[1].get("stop-color", "")
        
        img_tag = svg_element.find("image", id="giftPattern")
        if img_tag:
             pattern_png_url_val = img_tag.get("xlink:href") or img_tag.get("href")
        
        flood_tag = svg_element.find(id=re.compile(r"gift.*PatternColor", re.IGNORECASE))
        if flood_tag:
            pattern_tint = flood_tag.get("flood-color")

    pattern_png_url_val = normalize_url(pattern_png_url_val)
    tgs_url_val = normalize_url(tgs_url_val)

    return {
        "collectible_id": collectible_id,
        "nft_name": nft_name,
        "owner": attrs.get("owner", ""),
        "model": attrs.get("model", ""),
        "backdrop": attrs.get("backdrop", ""),
        "symbol": attrs.get("symbol", ""),
        "quantity": attrs.get("quantity", ""),
        "gradient_from": grad_from,
        "gradient_to": grad_to,
        "pattern_png_url": pattern_png_url_val,
        "pattern_tint": pattern_tint,
        "image_svg_b64": img_b64,
        "tgs_url": tgs_url_val,
    }


def download_unique(url: str | None,
                    dest_dir: Path, # This will be the collection-specific path e.g. .../script_dir/CollectionName_tgs
                    cache_dict: dict,
                    file_name_override: str | None = None,
                    retries: int = 3,
                    expected_ext: str | None = None,
                    proxy_url: str | None = None) -> str | None:
    if not url:
        return None
    
    normalized_fetch_url = normalize_url(url)
    if not normalized_fetch_url:
        tqdm.write(f"Не удалось нормализовать URL для скачивания: {url}")
        return None

    if normalized_fetch_url in cache_dict:
        return cache_dict[normalized_fetch_url]

    url_path_part = normalized_fetch_url.split("?", 1)[0]
    name_from_url_for_ext = os.path.basename(url_path_part)
    _, url_ext_from_path = os.path.splitext(name_from_url_for_ext)

    final_ext = url_ext_from_path.lower()
    if not final_ext and expected_ext:
        final_ext = expected_ext if expected_ext.startswith('.') else '.' + expected_ext
    elif not final_ext:
        final_ext = ".dat"

    actual_file_name_str: str
    if file_name_override:
        safe_base_name = re.sub(r'[^\w\.-]', '', file_name_override)
        safe_base_name = re.sub(r'\s+', '_', safe_base_name).strip('._-')

        base_name_no_ext, ext_in_override = os.path.splitext(safe_base_name)
        
        if ext_in_override.lower() == final_ext:
            actual_file_name_str = safe_base_name
        elif ext_in_override: 
            actual_file_name_str = base_name_no_ext + final_ext
        else: 
            actual_file_name_str = safe_base_name + final_ext
    else:
        base_name_hash = hashlib.md5(normalized_fetch_url.encode()).hexdigest()
        actual_file_name_str = base_name_hash + final_ext
    
    if not actual_file_name_str.strip():
        base_name_hash = hashlib.md5(normalized_fetch_url.encode()).hexdigest()
        actual_file_name_str = base_name_hash + final_ext
        tqdm.write(f"Предупреждение: file_name_override ('{file_name_override}') стал пустым после очистки. Используется имя на основе хеша: {actual_file_name_str}")

    # dest_dir.name will be "CollectionName_tgs" or "CollectionName_patterns"
    # This makes the stored path relative to script_dir, but including the collection-specific folder
    relative_file_path = Path(dest_dir.name) / actual_file_name_str
    full_dest_path = dest_dir / actual_file_name_str
    
    # dest_dir itself is created in process_one_collection

    if full_dest_path.exists() and full_dest_path.is_file():
        cache_dict[normalized_fetch_url] = str(relative_file_path)
        return str(relative_file_path)

    proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None

    for attempt in range(retries):
        try:
            r = requests.get(normalized_fetch_url, stream=True, timeout=20, headers=HEADERS, proxies=proxies)
            r.raise_for_status()
            
            with open(full_dest_path, "wb") as f:
                shutil.copyfileobj(r.raw, f)
            
            cache_dict[normalized_fetch_url] = str(relative_file_path)
            return str(relative_file_path)
        except requests.exceptions.Timeout:
            tqdm.write(f"Таймаут при скачивании {normalized_fetch_url} (попытка {attempt + 1}/{retries})")
            if attempt < retries - 1: time.sleep(2 + attempt * 2)
            else: tqdm.write(f"Не удалось скачать {normalized_fetch_url} после {retries} попыток (таймаут).")
        except requests.exceptions.RequestException as e:
            tqdm.write(f"Ошибка скачивания {normalized_fetch_url} (попытка {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1: time.sleep(1 + attempt)
            else: tqdm.write(f"Не удалось скачать {normalized_fetch_url} после {retries} попыток.")
        except OSError as e:
            tqdm.write(f"Ошибка файловой системы при сохранении {full_dest_path} (попытка {attempt + 1}/{retries}): {e}")
            if e.errno == 22 and len(actual_file_name_str) > 200 : 
                 tqdm.write(f"Критическая ошибка: слишком длинное имя файла '{actual_file_name_str}'. URL: {normalized_fetch_url}. Пропуск.")
                 return None 
            if attempt < retries - 1: time.sleep(1 + attempt)
            else: tqdm.write(f"Не удалось сохранить {full_dest_path} после {retries} попыток.")
        except Exception as e:
            tqdm.write(f"Неожиданная ошибка при скачивании/сохранении {normalized_fetch_url} (попытка {attempt + 1}/{retries}): {e}")
            if attempt < retries - 1: time.sleep(1 + attempt)
            else: tqdm.write(f"Не удалось обработать {normalized_fetch_url} после {retries} попыток.")
    return None


async def fetch_html_async(session: aiohttp.ClientSession, url: str, url_id: int, proxy_url: str | None, retries: int = 3) -> str | None:
    for attempt in range(retries):
        try:
            async with session.get(url, proxy=proxy_url, timeout=aiohttp.ClientTimeout(total=20)) as response:
                if response.status == 404:
                    tqdm.write(f"[{url_id}] NFT {url} не найден (404).")
                    return None
                response.raise_for_status()
                return await response.text()
        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                tqdm.write(f"[{url_id}] NFT {url} не найден (404 ClientResponseError).")
                return None
            tqdm.write(f"[{url_id}] Ошибка HTTP {url} (попытка {attempt + 1}/{retries}): {e.status} {e.message}")
        except asyncio.TimeoutError:
            tqdm.write(f"[{url_id}] Таймаут запроса {url} (попытка {attempt + 1}/{retries})")
        except aiohttp.ClientError as e:
            tqdm.write(f"[{url_id}] Ошибка клиента {url} (попытка {attempt + 1}/{retries}): {e}")
        except Exception as e:
            tqdm.write(f"[{url_id}] Неожиданная ошибка запроса {url} (попытка {attempt + 1}/{retries}): {type(e).__name__} {e}")
        
        if attempt < retries - 1:
            await asyncio.sleep(1 + attempt * 2)
        else:
            tqdm.write(f"[{url_id}] Не удалось получить {url} после {retries} попыток.")
    return None

async def fetch_and_process_page_async(
    session: aiohttp.ClientSession, 
    semaphore: asyncio.Semaphore, 
    url_id: int, 
    collection_slug: str, 
    base_url_template: str, 
    request_delay: float, 
    json_only_mode: bool, 
    tgs_dir_path_collection_specific: Path,  # Renamed for clarity
    pattern_dir_path_collection_specific: Path, # Renamed for clarity
    proxy_url: str | None,
    script_dir: Path
) -> dict | None:
    async with semaphore:
        page_url = base_url_template.format(url_id)
        html_content = await fetch_html_async(session, page_url, url_id, proxy_url)

        if not html_content:
            if request_delay > 0: await asyncio.sleep(request_delay)
            return None
        
        try:
            item_data = parse_page(html_content, url_id, collection_slug)
            if item_data is None:
                if request_delay > 0: await asyncio.sleep(request_delay)
                return None
        except Exception as e:
            tqdm.write(f"[{url_id}] Ошибка парсинга {page_url}: {e}")
            error_html_file = script_dir / f"error_page_{collection_slug}_{url_id}.html"
            try:
                with open(error_html_file, "w", encoding="utf-8") as ef:
                   ef.write(html_content)
                tqdm.write(f"HTML сохранен в {error_html_file.name}")
            except Exception as ex_save:
                tqdm.write(f"Не удалось сохранить error HTML: {ex_save}")
            if request_delay > 0: await asyncio.sleep(request_delay)
            return None

        tgs_url = item_data.get("tgs_url")
        pattern_png_url = item_data.get("pattern_png_url")

        if json_only_mode:
            item_data["tgs_file_path"] = tgs_url
            item_data["pattern_file_path"] = pattern_png_url
        else:
            tgs_filename_base = None
            model_name_raw = item_data.get("model", "")
            if model_name_raw:
                model_name_clean = model_name_raw.split('%')[0].strip()
                tgs_filename_base = re.sub(r'\s+', '_', model_name_clean)
                tgs_filename_base = re.sub(r'[^\w.-]', '', tgs_filename_base)

            item_data["tgs_file_path"] = download_unique(
                url=tgs_url,
                dest_dir=tgs_dir_path_collection_specific, # Use collection specific path
                cache_dict=downloaded_models_cache,
                file_name_override=tgs_filename_base,
                expected_ext=".tgs",
                proxy_url=proxy_url
            )

            pattern_filename_base = None
            pattern_symbol_text = item_data.get("symbol", "")
            if pattern_png_url:
                symbol_raw = pattern_symbol_text.split('%')[0].strip()
                symbol_clean = re.sub(r'[^A-Za-z0-9_-]+', '_', symbol_raw)
                symbol_clean = re.sub(r'_+', '_', symbol_clean).strip('_-')

                if symbol_clean:
                    pattern_filename_base = symbol_clean
                else:
                    url_hash_short = hashlib.md5(pattern_png_url.encode()).hexdigest()[:8]
                    pattern_filename_base = f"pattern_{url_hash_short}"
            
            item_data["pattern_file_path"] = download_unique(
                url=pattern_png_url,
                dest_dir=pattern_dir_path_collection_specific, # Use collection specific path
                cache_dict=downloaded_patterns_cache,
                file_name_override=pattern_filename_base, 
                expected_ext=".png",
                proxy_url=proxy_url
            )
        
        item_data["page_scraped_url"] = page_url 

        if request_delay > 0:
            await asyncio.sleep(request_delay)
        
        return item_data

async def scrape_collection_async(
    collection_slug: str, 
    id_first: int, 
    id_last: int, 
    request_delay: float, 
    json_only_mode: bool, 
    tgs_dir_path_coll_spec: Path,  # Renamed
    pattern_dir_path_coll_spec: Path, # Renamed
    proxy_url: str | None, 
    num_workers: int,
    script_dir: Path
) -> list[dict]:
    current_base_url_template = f"https://t.me/nft/{collection_slug}-{{}}"
    
    connector = aiohttp.TCPConnector(limit_per_host=num_workers, ssl=True, force_close=True)
    
    async with aiohttp.ClientSession(headers=HEADERS, connector=connector) as session:
        semaphore = asyncio.Semaphore(num_workers)
        tasks = []
        for i in range(id_first, id_last + 1):
            tasks.append(
                fetch_and_process_page_async(
                    session, semaphore, i, collection_slug, current_base_url_template, 
                    request_delay, json_only_mode, 
                    tgs_dir_path_coll_spec, # Pass collection specific path
                    pattern_dir_path_coll_spec, # Pass collection specific path
                    proxy_url, script_dir
                )
            )
        
        results = []
        for f in async_tqdm.as_completed(tasks, total=len(tasks), desc=f"Scraping {collection_slug} (IDs {id_first}-{id_last})"):
            item = await f
            if item:
                results.append(item)
        return results

def get_total_issued(collection_slug: str, proxy_url: str | None, start_nft_id: int = 1) -> int | None:
    url = f"https://t.me/nft/{collection_slug}-{start_nft_id}"
    proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
    tqdm.write(f"Определение общего количества NFT для '{collection_slug}' со страницы {url}...")
    try:
        response = requests.get(url, headers=HEADERS, timeout=20, proxies=proxies)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        quantity_th = soup.find("th", string=lambda text: text and "quantity" in text.lower())
        if not quantity_th:
            tqdm.write(f"Не удалось найти 'Quantity' в таблице на {url}")
            return None
        
        quantity_td = quantity_th.find_next_sibling("td")
        if not quantity_td:
            tqdm.write(f"Не удалось найти значение для 'Quantity' на {url}")
            return None
            
        quantity_text = quantity_td.get_text(" ", strip=True)
        match = re.match(r"([\d\s,\u00A0]+)(?:/\s*[\d\s,\u00A0]+)?\s*issued", quantity_text, re.IGNORECASE)
        if match:
            total_issued_str = match.group(1).replace(" ", "").replace(",", "").replace("\u00A0", "")
            return int(total_issued_str)
        else:
            tqdm.write(f"Не удалось извлечь общее количество из текста: '{quantity_text}' на {url}")
            return None

    except requests.exceptions.RequestException as e:
        tqdm.write(f"Ошибка при запросе общего количества для '{collection_slug}': {e}")
        return None
    except ValueError as e:
        tqdm.write(f"Ошибка при конвертации общего количества в число для '{collection_slug}': {e}")
        return None
    except Exception as e:
        tqdm.write(f"Неожиданная ошибка при получении общего количества для '{collection_slug}': {e}")
        return None


def process_one_collection(
    collection_slug: str, 
    id_first: int, 
    id_last: int, 
    output_file: str, 
    request_delay: float, 
    json_only_mode: bool,
    proxy_url: str | None,
    num_workers: int,
    auto_last: bool,
    start_nft_id_for_total: int
):
    script_dir = Path(__file__).parent.resolve()

    # Dynamically create directory names based on collection_slug
    # Ensure slug is reasonably safe for directory names (Telegram slugs usually are)
    safe_slug_for_dirname = re.sub(r'[^\w-]+', '_', collection_slug) # Basic sanitization for safety
    tgs_dir_name_dynamic = f"{safe_slug_for_dirname}_tgs"
    pattern_dir_name_dynamic = f"{safe_slug_for_dirname}_patterns"

    tgs_dir_path_collection_specific = script_dir / tgs_dir_name_dynamic
    pattern_dir_path_collection_specific = script_dir / pattern_dir_name_dynamic


    if auto_last:
        print(f"[{collection_slug}] Автоматическое определение последнего ID...")
        total_issued = get_total_issued(collection_slug, proxy_url, start_nft_id_for_total)
        if total_issued is not None and total_issued > 0 :
            id_last = total_issued
            print(f"[{collection_slug}] Обнаружено {id_last} NFT. Будет произведен парсинг с ID {id_first} по {id_last}.")
            if id_first > id_last:
                print(f"[{collection_slug}] Предупреждение: Начальный ID ({id_first}) больше, чем обнаруженный последний ID ({id_last}). Пропуск коллекции.")
                return
        else:
            print(f"[{collection_slug}] Не удалось автоматически определить последний ID. Используется указанный --last: {id_last} (или значение по умолчанию).")

    if id_last < id_first:
        print(f"[{collection_slug}] Ошибка: Конечный ID ({id_last}) не может быть меньше начального ({id_first}). Пропуск коллекции.")
        return

    if not json_only_mode:
        tgs_dir_path_collection_specific.mkdir(parents=True, exist_ok=True)
        pattern_dir_path_collection_specific.mkdir(parents=True, exist_ok=True)
    else:
        tqdm.write(f"[{collection_slug}] Режим JSON-only: Скачивание файлов TGS и паттернов отключено.")
    
    processed_data = {} 
    output_file_path = script_dir / output_file

    if output_file_path.exists() and output_file_path.stat().st_size > 0:
        try:
            with open(output_file_path, "r", encoding="utf-8") as f:
                existing_items = json.load(f)
                if isinstance(existing_items, list):
                    for item in existing_items:
                        if isinstance(item, dict) and "collectible_id" in item and "page_scraped_url" in item:
                            if f"/{collection_slug}-" in item["page_scraped_url"]:
                                processed_data[item["collectible_id"]] = item
                    tqdm.write(f"[{collection_slug}] Загружено {len(processed_data)} существующих записей из {output_file_path.name}")
                else:
                    tqdm.write(f"[{collection_slug}] Предупреждение: Файл {output_file_path.name} не содержит JSON список. Данные будут записываться заново.")
        except json.JSONDecodeError:
            tqdm.write(f"[{collection_slug}] Предупреждение: Файл {output_file_path.name} поврежден. Данные будут записываться заново.")
        except Exception as e:
            tqdm.write(f"[{collection_slug}] Предупреждение: Не удалось загрузить данные из {output_file_path.name}: {e}. Данные будут записываться заново.")
    
    newly_scraped_count = 0
    updated_count = 0

    scraped_items_list = asyncio.run(scrape_collection_async(
        collection_slug, id_first, id_last, request_delay, json_only_mode,
        tgs_dir_path_collection_specific, # Pass collection specific path
        pattern_dir_path_collection_specific, # Pass collection specific path
        proxy_url, num_workers, script_dir
    ))

    for item_data in scraped_items_list:
        if item_data is None:
            continue
        collectible_id = item_data["collectible_id"]
        if collectible_id not in processed_data:
            newly_scraped_count += 1
        else:
            if json.dumps(processed_data[collectible_id], sort_keys=True) != json.dumps(item_data, sort_keys=True):
                 updated_count +=1
        processed_data[collectible_id] = item_data

    all_items_data_list = list(processed_data.values())
    all_items_data_list.sort(key=lambda x: (
        int(x.get("collectible_id", float('inf'))) if isinstance(x.get("collectible_id"), (int, str)) and str(x.get("collectible_id")).isdigit() 
        else float('inf')
    ))

    try:
        with open(output_file_path, "w", encoding="utf-8") as f:
            json.dump(all_items_data_list, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[{collection_slug}] Ошибка при записи JSON в файл {output_file_path.name}: {e}")
        backup_path = output_file_path.with_suffix(f".backup_{int(time.time())}.json")
        try:
            with open(backup_path, "w", encoding="utf-8") as bf:
                json.dump(all_items_data_list, bf, ensure_ascii=False, indent=2)
            print(f"[{collection_slug}] Данные для текущего запуска сохранены в бэкап: {backup_path.name}")
        except Exception as be:
            print(f"[{collection_slug}] Не удалось сохранить бэкап: {be}")
        return

    print(f"\n✓ [{collection_slug}] Готово! Всего {len(all_items_data_list):,} записей сохранено/обновлено в {output_file_path.name}")
    print(f"  Из них {newly_scraped_count} новых, {updated_count} обновленных в этом запуске для '{collection_slug}'.")
    
    if not json_only_mode:
        print(f"  TGS модели для '{collection_slug}' сохраняются в: '{tgs_dir_path_collection_specific.resolve()}'")
        print(f"  PNG паттерны для '{collection_slug}' сохраняются в: '{pattern_dir_path_collection_specific.resolve()}'")
    else:
        print(f"  [{collection_slug}] Скачивание файлов TGS и PNG было отключено (режим JSON-only).")
        print(f"  Поля 'tgs_file_path' и 'pattern_file_path' в JSON содержат прямые URL (если доступны).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Скрейпер данных NFT с t.me/nft/.")
    
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--slug", type=str, help="Slug ОДНОЙ коллекции NFT (например, AstralShard).")
    group.add_argument("--slugs", nargs="+", help="Список слагов коллекций NFT через пробел (например, AstralShard Collection2).")

    parser.add_argument("--first", type=int, default=1, help="Начальный ID для скрейпинга.")
    parser.add_argument("--last", type=int, default=10, help="Конечный ID для скрейпинга (включительно). Игнорируется, если --auto-last успешно определяет количество.")
    parser.add_argument("--output", type=str, default="nft_collection_data.json", help="Имя выходного JSON файла (используется только с --slug, для --slugs имена генерируются автоматически).")
    parser.add_argument("--delay", type=float, default=0.1, help="Задержка между запросами КАЖДОГО воркера в секундах (0 для отключения).")
    parser.add_argument("--json-only", action="store_true", help="Только генерировать JSON данные, не скачивать файлы TGS/паттернов.")
    parser.add_argument("--workers", type=int, default=10, help="Количество одновременных воркеров для HTTP запросов.")
    parser.add_argument("--proxy", type=str, default=None, help="URL прокси-сервера (например, http://user:pass@host:port).")
    parser.add_argument("--auto-last", action="store_true", help="Автоматически определять последний ID для каждой коллекции, игнорируя --last.")
    parser.add_argument("--start-nft-id-for-total", type=int, default=1, help="ID NFT, используемый для определения общего количества при --auto-last (обычно 1).")

    args = parser.parse_args()

    if not args.slug and not args.slugs:
        parser.error("Необходимо указать --slug или --slugs.")
    
    if args.last < args.first and not args.auto_last :
        print("Ошибка: Значение 'last' не может быть меньше 'first' (если не используется --auto-last).")
        exit(1)

    script_dir_display = Path(__file__).parent.resolve()
    print(f"Запуск скрейпера...")
    print(f"Папка для результатов: {script_dir_display}")
    print(f"Задержка на воркер: {args.delay} сек. Количество воркеров: {args.workers}")
    if args.proxy:
        print(f"Используется прокси: {args.proxy}")
    if args.auto_last:
        print(f"Включено автоматическое определение последнего ID (ID для проверки: {args.start_nft_id_for_total}). Параметр --last будет проигнорирован.")
    
    if args.json_only:
        print("Режим JSON-only: Файлы TGS и PNG не будут скачиваться.")
    else:
        print(f"TGS модели будут сохраняться в папки вида: 'ИМЯ-КОЛЛЕКЦИИ_tgs' (относительно {script_dir_display})")
        print(f"PNG паттерны будут сохраняться в папки вида: 'ИМЯ-КОЛЛЕКЦИИ_patterns' (относительно {script_dir_display})")

    collection_targets = []
    if args.slugs:
        for slug_item in args.slugs:
            safe_slug_for_filename = re.sub(r'[^\w-]+', '_', slug_item)
            collection_targets.append({
                "slug": slug_item,
                "output_file": f"{safe_slug_for_filename}_collection_data.json"
            })
    elif args.slug:
        collection_targets.append({
            "slug": args.slug,
            "output_file": args.output
        })
    
    downloaded_models_cache.clear()
    downloaded_patterns_cache.clear()

    start_time = time.time()
    total_processed_collections = 0

    for target in collection_targets:
        current_slug = target["slug"]
        current_output_file = target["output_file"]
        
        print(f"\n{'='*10} Обработка коллекции: {current_slug} {'='*10}")
        print(f"Результаты будут сохранены/обновлены в: {script_dir_display / current_output_file}")
        
        process_one_collection(
            collection_slug=current_slug,
            id_first=args.first,
            id_last=args.last,
            output_file=current_output_file,
            request_delay=args.delay,
            json_only_mode=args.json_only,
            proxy_url=args.proxy,
            num_workers=args.workers,
            auto_last=args.auto_last,
            start_nft_id_for_total=args.start_nft_id_for_total
        )
        total_processed_collections +=1
    
    end_time = time.time()
    print(f"\n{'='*10} Завершено {'='*10}")
    print(f"Всего обработано коллекций: {total_processed_collections}")
    print(f"Общее время выполнения: {end_time - start_time:.2f} секунд.")
    if not args.json_only:
        print(f"Всего уникальных TGS URL обработано (скачано/кэшировано) за весь запуск: {len(downloaded_models_cache)}")
        print(f"Всего уникальных Pattern URL обработано (скачано/кэшировано) за весь запуск: {len(downloaded_patterns_cache)}")