import pandas as pd
import os
import requests  # Masih dipakai untuk requests.utils.quote
from bs4 import BeautifulSoup
import time
from tqdm import tqdm
# openpyxl tidak lagi diimpor langsung untuk styling, tapi pandas .style.to_excel butuh terinstal
import re
import random  # Import modul random untuk jeda acak

try:
    import cloudscraper

    CLOUDSCAPER_AVAILABLE = True
except ImportError:
    CLOUDSCAPER_AVAILABLE = False
    # Menggunakan simbol untuk peringatan
    print(
        "⚠️ --- PERINGATAN: Library cloudscraper tidak terinstal. Scraping nomor.net akan menggunakan requests biasa (kemungkinan gagal karena Cloudflare). ---")
    print("--- Untuk mencoba melewati Cloudflare di nomor.net, jalankan: pip install cloudscraper ---")

# Inisialisasi Colorama untuk output berwarna di konsol (opsional, jika ingin warna juga)
# Jika tidak pakai colorama, simbol saja yang akan tampil.
try:
    import colorama
    from colorama import Fore, Style

    colorama.init(autoreset=True)  # autoreset=True agar style kembali normal setelah setiap print
    COLORAMA_AVAILABLE = True
    COLOR_SUCCESS = Fore.GREEN
    COLOR_ERROR = Fore.RED
    COLOR_WARNING = Fore.YELLOW
    COLOR_INFO = Fore.CYAN
    COLOR_RESET = Style.RESET_ALL
except ImportError:
    COLORAMA_AVAILABLE = False
    COLOR_SUCCESS = ""
    COLOR_ERROR = ""
    COLOR_WARNING = ""
    COLOR_INFO = ""
    COLOR_RESET = ""
    # Tidak perlu print peringatan colorama lagi jika kita fokus ke simbol

# --- SIMBOL UNTUK DEBUG ---
SYM_SUCCESS = "✅"
SYM_ERROR = "❌"
SYM_INFO = "ℹ️"
SYM_WARNING = "⚠️"

# --- DAFTAR USER-AGENT UNTUK ROTASI ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36"
]

# Path ke file Excel
try:
    base_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    base_dir = os.getcwd()

data_folder_path = os.path.join(base_dir, "../data")
input_file = os.path.join(data_folder_path, "need_to_fix_part1.xlsx")  # Sesuai input Chief

# --- KONFIGURASI PENGGUNA UNTUK MANUAL CHUNKING ---
MY_PROCESS_ID = "vincent"  # Sesuai kode user
MY_START_ROW_ABSOLUTE = 118
MY_END_ROW_EXCLUSIVE_ABSOLUTE = 998
# -------------------------------------------------

BATCH_SIZE = 20
MAX_RETRIES_NOMOR_NET = 2
RETRY_DELAY_NOMOR_NET_MIN = 5
RETRY_DELAY_NOMOR_NET_MAX = 10

# --- Nama File Output & Folder Batch ---
output_file_suffix = f"_part_{MY_PROCESS_ID}_{MY_START_ROW_ABSOLUTE + 1}-{MY_END_ROW_EXCLUSIVE_ABSOLUTE}"
# Versi dinaikkan untuk menandai perubahan simbol debug dan penghapusan simpan HTML
output_file = os.path.join(data_folder_path, f"village_postal_code_v15{output_file_suffix}.xlsx")
batches_dir = os.path.join(data_folder_path, f"batches_v15{output_file_suffix}")
# debug_html_storage_dir dihapus

# --- NAMA KOLOM DARI FILE INPUT (Pastikan sesuai dengan file Excel Chief) ---
COL_ID_DESA = 'ID Desa (Village ID)'
COL_NAMA_DESA = 'Nama Desa (Village Name)'
COL_KECAMATAN = 'Kecamatan (District)'
COL_KABUPATEN = 'Kabupaten (Regency)'

# --- DEBUGGING FLAG ---
DEBUG_TARGET_VILLAGE = "Lamtui"
DEBUG_TARGET_DISTRICT = "Kuta Cot Glie"
ENABLE_DETAILED_DEBUG = True  # Tetap True untuk log konsol dengan simbol

print(f"{SYM_INFO} --- DEBUG: Base directory (lokasi skrip): {base_dir} ---")
print(f"{SYM_INFO} --- DEBUG: Data directory: {data_folder_path} ---")
print(f"{SYM_INFO} --- DEBUG: Target input file: {input_file} ---")
print(f"{SYM_INFO} --- DEBUG (ID: {MY_PROCESS_ID}): Target output file: {output_file} ---")
print(f"{SYM_INFO} --- DEBUG (ID: {MY_PROCESS_ID}): Target batches directory: {batches_dir} ---")
# Print untuk debug_html_storage_dir dihapus

# Membuat folder batches jika belum ada
try:
    if not os.path.exists(batches_dir):
        os.makedirs(batches_dir);
        print(f"{SYM_SUCCESS} Folder {batches_dir} telah berhasil dibuat.")
    else:
        print(f"{SYM_INFO} Folder {batches_dir} sudah ada.")
except Exception as e:
    print(f"{SYM_ERROR} --- ERROR: Gagal membuat folder batches: {batches_dir} - Error: {e} ---");
    exit(1)

try:
    df_full = pd.read_excel(input_file, sheet_name="villages")
    print(f"{SYM_INFO} File {input_file} berhasil dibaca. Jumlah baris total di Excel: {len(df_full)}")
    actual_start_row = max(0, MY_START_ROW_ABSOLUTE);
    actual_end_row = min(len(df_full), MY_END_ROW_EXCLUSIVE_ABSOLUTE)
    if actual_start_row < actual_end_row:
        print(
            f"{SYM_INFO} --- INFO (ID: {MY_PROCESS_ID}): Proses baris {actual_start_row} hingga {actual_end_row - 1} ---")
        df = df_full.iloc[actual_start_row:actual_end_row].reset_index(drop=True)
        print(f"{SYM_INFO} --- INFO (ID: {MY_PROCESS_ID}): Jumlah baris diproses: {len(df)} ---")
    else:
        df = pd.DataFrame();
        print(f"{SYM_WARNING} --- INFO (ID: {MY_PROCESS_ID}): Rentang baris tidak valid/kosong. ---")
except Exception as e:
    print(f"{SYM_ERROR} Error baca Excel/chunk: {e}");
    exit(1)

if df.empty: print(f"{SYM_WARNING} Tidak ada data diproses (ID: {MY_PROCESS_ID}). Skrip berhenti."); exit(0)


# Fungsi format ID Desa ke format Kode Wilayah (XX.XX.XX.XXXX)
def format_id_desa_to_kode_wilayah(id_desa_val):
    if pd.isna(id_desa_val): return None
    id_desa_str = str(id_desa_val).strip()
    if id_desa_str.endswith(".0"): id_desa_str = id_desa_str[:-2]
    if len(id_desa_str) == 10 and id_desa_str.isdigit():
        return f"{id_desa_str[:2]}.{id_desa_str[2:4]}.{id_desa_str[4:6]}.{id_desa_str[6:]}"
    elif re.fullmatch(r"\d{2}\.\d{2}\.\d{2}\.\d{4}", id_desa_str):
        return id_desa_str
    return None


# Fungsi buat generate URL nomor.net (FORMAT DETAIL BARU - REVISED)
def generate_nomor_url_detailed(row):
    vil_original = str(row.get(COL_NAMA_DESA, "")).strip()
    dist_original = str(row.get(COL_KECAMATAN, "")).strip()
    reg_original = str(row.get(COL_KABUPATEN, "")).strip()

    if not vil_original or not dist_original or not reg_original:
        return "URL (detail) tidak dibuat (data kurang)"

    reg_cleaned = reg_original
    if reg_original.lower().startswith("kab. "):
        reg_cleaned = reg_original[5:].strip()
    elif reg_original.lower().startswith("kota "):
        reg_cleaned = reg_original[5:].strip()

    dist_for_daerah = dist_original
    if dist_original.lower() == "kuta cot glie":
        dist_for_daerah = "Kuta Cot Glie (Kota Cot Glie)"
        if ENABLE_DETAILED_DEBUG: print(
            f"{SYM_INFO} DEBUG (generate_nomor_url_detailed): Penyesuaian nama kecamatan untuk '{dist_original}' menjadi '{dist_for_daerah}'")

    daerah_str = f"Desa-{dist_for_daerah}-Kab.-{reg_cleaned}"
    daerah_encoded = requests.utils.quote(daerah_str)
    jobs_encoded = requests.utils.quote(vil_original)

    generated_url = f"https://www.nomor.net/_kodepos.php?_i=desa-kodepos&sby=010000&daerah={daerah_encoded}&jobs={jobs_encoded}"

    if ENABLE_DETAILED_DEBUG:
        if (DEBUG_TARGET_VILLAGE and DEBUG_TARGET_VILLAGE.lower() in vil_original.lower()) or \
                (DEBUG_TARGET_DISTRICT and DEBUG_TARGET_DISTRICT.lower() in dist_original.lower()):
            print(
                f"\n{SYM_INFO} --- DEBUG: generate_nomor_url_detailed untuk '{vil_original}, {dist_original}, {reg_original}' ---")
            print(f"   Daerah String (sebelum encode): '{daerah_str}'")
            print(f"   Jobs String (sebelum encode): '{vil_original}'")
            print(f"   Generated URL: {generated_url}")
            print(f"-----------------------------------------------------------------------\n")
    return generated_url


# Fungsi buat generate URL nomor.net (FORMAT KODE WILAYAH)
def generate_nomor_url_by_kode_wilayah(row):
    id_desa_val = row.get(COL_ID_DESA)
    kode_wilayah_formatted = format_id_desa_to_kode_wilayah(id_desa_val)
    if not kode_wilayah_formatted:
        return "URL (kode wilayah) tidak dibuat (ID Desa tidak valid/kosong)"
    jobs_encoded = requests.utils.quote(kode_wilayah_formatted)
    return f"https://www.nomor.net/_kodepos.php?_i=cari-kodepos&jobs={jobs_encoded}&urut=8&sby=010000&no1a=2&no2a=&perhal=0&kk=0"


# Fungsi buat scraping kode pos dari nomor.net (MENGGUNAKAN cloudscraper dengan retry)
def scrape_nomor(url, url_type="detail", is_debug_target=False, current_village_name=""):
    if not url or not isinstance(url, str) or not url.startswith("http") or "URL tidak dapat dibuat" in url:
        if ENABLE_DETAILED_DEBUG: print(
            f"{SYM_WARNING} DEBUG (scrape_nomor {url_type}): URL tidak valid, skipping: {url}")
        return None
    if not CLOUDSCAPER_AVAILABLE:
        if ENABLE_DETAILED_DEBUG: print(
            f"{SYM_ERROR} DEBUG (scrape_nomor {url_type}): cloudscraper tidak tersedia untuk URL: {url}")
        return "Error: cloudscraper N/A"

    last_error_message = f"Gagal setelah beberapa percobaan ({url_type} - nomor.net)"
    for attempt in range(MAX_RETRIES_NOMOR_NET):
        chosen_ua = random.choice(USER_AGENTS);
        scraper = cloudscraper.create_scraper(browser={'custom': chosen_ua})
        time.sleep(random.uniform(1.5, 4.0))
        if ENABLE_DETAILED_DEBUG:
            print(
                f"\n{SYM_INFO} --- DEBUG: scrape_nomor ({url_type}, UA: {chosen_ua}, Percobaan {attempt + 1}/{MAX_RETRIES_NOMOR_NET}) ---")
            print(f"Mencoba scrape dari URL: {url} untuk Desa: {current_village_name}")
        try:
            response = scraper.get(url, timeout=30)
            if ENABLE_DETAILED_DEBUG: print(
                f"{SYM_INFO} DEBUG (scrape_nomor {url_type} - cloudscraper): Status Code: {response.status_code} untuk {url}")
            soup = BeautifulSoup(response.text, "html.parser")

            # Blok penyimpanan HTML dihapus
            # if ENABLE_DETAILED_DEBUG:
            #     try:
            #         ... (kode simpan HTML) ...
            #     except Exception as e_html:
            #         ...

            response.raise_for_status()
            code_found_this_attempt = None;
            postal_code_tag = soup.find("a", class_="ktw")
            if postal_code_tag and postal_code_tag.string:
                code = postal_code_tag.string.strip()
                if ENABLE_DETAILED_DEBUG: print(
                    f"{SYM_INFO} DEBUG (scrape_nomor {url_type} - cloudscraper): Tag <a class='ktw'>: '{code}'")
                if code.isdigit() and len(code) == 5:
                    code_found_this_attempt = code
                elif ENABLE_DETAILED_DEBUG:
                    print(
                        f"{SYM_WARNING} DEBUG (scrape_nomor {url_type} - cloudscraper): Kode dari ktw tdk valid: '{code}'")
            if not code_found_this_attempt:
                if ENABLE_DETAILED_DEBUG: print(
                    f"{SYM_INFO} DEBUG (scrape_nomor {url_type} - cloudscraper): Tag <a class='ktw'> tdk ada/valid. Fallback teks 5 digit...")
                all_text_nodes = soup.find_all(string=True)
                for text_node in all_text_nodes:
                    stripped_text = str(text_node).strip()
                    if stripped_text.isdigit() and len(stripped_text) == 5:
                        is_valid_context_fallback = True
                        if url_type == "detail":
                            parent_context_fallback = text_node.parent.get_text(separator=" ",
                                                                                strip=True).lower() if text_node.parent else ""
                            village_name_lower_fallback = current_village_name.lower()
                            if not (
                                    village_name_lower_fallback in parent_context_fallback or "kodepos" in parent_context_fallback or "kode pos" in parent_context_fallback):
                                is_valid_context_fallback = False
                        if is_valid_context_fallback:
                            if ENABLE_DETAILED_DEBUG: print(
                                f"{SYM_SUCCESS} DEBUG (scrape_nomor {url_type} - cloudscraper): Fallback - kode pos teks: {stripped_text}")
                            code_found_this_attempt = stripped_text;
                            break
            if code_found_this_attempt: print(
                f"{SYM_SUCCESS} Sukses ({url_type}): {current_village_name} -> {code_found_this_attempt}"); return code_found_this_attempt
            last_error_message = f"Tidak ditemukan kode pos ({url_type} - setelah parse)"
            if ENABLE_DETAILED_DEBUG: print(
                f"{SYM_WARNING} DEBUG (scrape_nomor {url_type}, Att {attempt + 1}): {last_error_message} u/ {current_village_name}")
        except requests.exceptions.HTTPError as http_err:
            last_error_message = f"Error HTTP ({http_err.response.status_code}) ({url_type})"
            if http_err.response.status_code == 403:
                last_error_message = f"Error 403 (Cloudflare block) ({url_type})"
            elif http_err.response.status_code == 404:
                if ENABLE_DETAILED_DEBUG: print(
                    f"{SYM_ERROR} DEBUG (scrape_nomor {url_type} - cloudscraper): HTTPError 404 untuk {url}. Tidak coba lagi.")
                return f"Halaman tidak ditemukan (404) ({url_type})"
            if ENABLE_DETAILED_DEBUG: print(
                f"{SYM_ERROR} DEBUG (scrape_nomor {url_type}, Att {attempt + 1}): HTTPError: {http_err} u/ {current_village_name}")
        except Exception as e:
            last_error_message = f"Error Lainnya (cloudscraper) ({url_type})"
            if ENABLE_DETAILED_DEBUG: print(
                f"{SYM_ERROR} DEBUG (scrape_nomor {url_type}, Att {attempt + 1}): Exception: {e} u/ {current_village_name}")
        if attempt < MAX_RETRIES_NOMOR_NET - 1:
            current_retry_delay = random.uniform(RETRY_DELAY_NOMOR_NET_MIN, RETRY_DELAY_NOMOR_NET_MAX)
            if ENABLE_DETAILED_DEBUG: print(
                f"{SYM_WARNING} DEBUG (scrape_nomor {url_type}): Att {attempt + 1} gagal ({last_error_message}). Jeda {current_retry_delay:.2f} dtk...")
            time.sleep(current_retry_delay)
        else:
            print(f"{SYM_ERROR} Gagal ({url_type}): {current_village_name} - {last_error_message}")
            return last_error_message
    return last_error_message


KODE_POS_RESULT_COL = "Kode Pos (Postal Code)"


def highlight_invalid_rows(row_series):
    kode_pos_value = str(row_series.get(KODE_POS_RESULT_COL, ""));
    is_invalid = False
    if not (kode_pos_value.isdigit() and len(kode_pos_value) == 5): is_invalid = True
    if is_invalid:
        return ['background-color: yellow'] * len(row_series)
    else:
        return [''] * len(row_series)


def save_batch_data(df_slice_to_save, batch_number_val):
    batch_file = os.path.join(batches_dir, f"villages_batch_{batch_number_val}.xlsx")
    try:
        styled_df_slice = df_slice_to_save.style.apply(highlight_invalid_rows, axis=1)
        styled_df_slice.to_excel(batch_file, index=False, engine='openpyxl')
        print(
            f"{SYM_SUCCESS} Batch {batch_number_val} (kumulatif u/ chunk {MY_PROCESS_ID}) disimpan & di-style di {batch_file}")
    except Exception as e:
        print(f"{SYM_ERROR}--- ERROR (save_batch_data): Gagal simpan/style batch {batch_file}: {e} ---")


# --- BAGIAN UTAMA SKRIP ---
URL_NOMOR_COL_DETAIL = "URL nomor.net (Detail)"
URL_NOMOR_COL_KODEWIL = "URL nomor.net (KodeWil)"

df[URL_NOMOR_COL_DETAIL] = df.apply(generate_nomor_url_detailed, axis=1)
df[URL_NOMOR_COL_KODEWIL] = df.apply(generate_nomor_url_by_kode_wilayah, axis=1)
if KODE_POS_RESULT_COL not in df.columns:
    df[KODE_POS_RESULT_COL] = ""
else:
    df[KODE_POS_RESULT_COL] = ""

print(f"\n{SYM_INFO} Contoh URL nomor.net (Detail - 5 pertama dari chunk {MY_PROCESS_ID}):");
print(df[URL_NOMOR_COL_DETAIL].head().tolist());
print(f"\n{SYM_INFO} Contoh URL nomor.net (KodeWil - 5 pertama dari chunk {MY_PROCESS_ID}):");
print(df[URL_NOMOR_COL_KODEWIL].head().tolist());
print("-" * 30)
batch_size_val = int(BATCH_SIZE) if str(BATCH_SIZE).isdigit() else 20;
batch_number = 1
print(
    f"\n{SYM_INFO} Memulai scraping (ID: {MY_PROCESS_ID}, Jatah: {MY_START_ROW_ABSOLUTE} s.d. {MY_END_ROW_EXCLUSIVE_ABSOLUTE - 1})...")

for i in tqdm(range(len(df)), desc=f"Scraping Progress (ID: {MY_PROCESS_ID})"):
    url_detail_current = df.at[i, URL_NOMOR_COL_DETAIL]
    url_kodewil_current = df.at[i, URL_NOMOR_COL_KODEWIL]
    current_village_name_for_debug = str(df.at[i, COL_NAMA_DESA]) if COL_NAMA_DESA in df.columns else f"baris_index_{i}"

    final_result_this_row = None;
    is_current_row_debug_target = False
    if ENABLE_DETAILED_DEBUG:
        try:
            vil_check = str(df.at[i, COL_NAMA_DESA]).lower().strip() if COL_NAMA_DESA in df.columns and pd.notna(
                df.at[i, COL_NAMA_DESA]) else ""
            dist_check = str(df.at[i, COL_KECAMATAN]).lower().strip() if COL_KECAMATAN in df.columns and pd.notna(
                df.at[i, COL_KECAMATAN]) else ""
            is_current_row_debug_target = (DEBUG_TARGET_VILLAGE and DEBUG_TARGET_VILLAGE.lower() in vil_check) and \
                                          (not DEBUG_TARGET_DISTRICT or (
                                                      DEBUG_TARGET_DISTRICT and DEBUG_TARGET_DISTRICT.lower() in dist_check))
        except KeyError:
            is_current_row_debug_target = False

    if is_current_row_debug_target and ENABLE_DETAILED_DEBUG:
        original_idx_debug = MY_START_ROW_ABSOLUTE + i
        print(
            f"\n{SYM_INFO} --- PROCESSING DEBUG TARGET ROW (ID: {MY_PROCESS_ID}, Indeks Asli: {original_idx_debug + 1}, Slice: {i}) ---")
        print(f"Desa: {current_village_name_for_debug}");
        print(f"URL Detail: {url_detail_current}");
        print(f"URL KodeWil: {url_kodewil_current}")

    id_desa_present_for_row = pd.notna(df.at[i, COL_ID_DESA]) and str(df.at[i, COL_ID_DESA]).strip() != ""

    if id_desa_present_for_row and url_kodewil_current and "URL tidak dapat dibuat" not in url_kodewil_current:
        if ENABLE_DETAILED_DEBUG: print(
            f"{SYM_INFO} DEBUG (baris {i}, Desa: {current_village_name_for_debug}): Mencoba URL Kode Wilayah dulu: {url_kodewil_current}")
        kodewil_res = scrape_nomor(url_kodewil_current, is_debug_target=is_current_row_debug_target,
                                   current_village_name=current_village_name_for_debug, url_type="kodewil")
        if kodewil_res and isinstance(kodewil_res, str) and kodewil_res.isdigit() and len(kodewil_res) == 5:
            final_result_this_row = kodewil_res
        else:
            final_result_this_row = kodewil_res
            if ENABLE_DETAILED_DEBUG: print(
                f"{SYM_WARNING} DEBUG (baris {i}, Desa: {current_village_name_for_debug}): Hasil URL KodeWil: {kodewil_res}. Akan mencoba URL Detail...")

    if not (final_result_this_row and isinstance(final_result_this_row,
                                                 str) and final_result_this_row.isdigit() and len(
            final_result_this_row) == 5):
        error_from_kodewil = final_result_this_row
        if ENABLE_DETAILED_DEBUG and not (
                id_desa_present_for_row and url_kodewil_current and "URL tidak dapat dibuat" not in url_kodewil_current):
            print(
                f"{SYM_INFO} DEBUG (baris {i}, Desa: {current_village_name_for_debug}): URL Kode Wilayah tidak dicoba/tidak valid. Mencoba URL Detail: {url_detail_current}")
        detail_res = scrape_nomor(url_detail_current, is_debug_target=is_current_row_debug_target,
                                  current_village_name=current_village_name_for_debug, url_type="detail")
        if detail_res and isinstance(detail_res, str) and detail_res.isdigit() and len(detail_res) == 5:
            final_result_this_row = detail_res
        elif detail_res:
            final_result_this_row = detail_res
        elif error_from_kodewil:
            final_result_this_row = error_from_kodewil
        else:
            final_result_this_row = "Invalid Data (kedua URL nomor.net gagal)"
    df.loc[i, KODE_POS_RESULT_COL] = final_result_this_row

    if is_current_row_debug_target and ENABLE_DETAILED_DEBUG:
        if final_result_this_row and isinstance(final_result_this_row, str) and final_result_this_row.isdigit() and len(
                final_result_this_row) == 5:
            print(f"{SYM_SUCCESS} Hasil Kode Pos u/ Target Debug (Slice {i}): {df.loc[i, KODE_POS_RESULT_COL]}");
            print(f"--- END DEBUG TARGET ROW ---\n")
        else:
            print(f"{SYM_ERROR} Hasil Kode Pos u/ Target Debug (Slice {i}): {df.loc[i, KODE_POS_RESULT_COL]}");
            print(f"--- END DEBUG TARGET ROW ---\n")

    if (i + 1) % batch_size_val == 0 or ((i + 1) == len(df) and len(df) > 0):
        start_idx_cumulative_slice = 0;
        end_idx_cumulative_slice = i + 1
        df_cumulative_slice_to_save = df.iloc[start_idx_cumulative_slice:end_idx_cumulative_slice].copy()
        if not df_cumulative_slice_to_save.empty: save_batch_data(df_cumulative_slice_to_save, batch_number)
        batch_number += 1
print(f"\n{SYM_INFO} Menyimpan hasil ID: {MY_PROCESS_ID} ({len(df)} baris) ke {output_file}...")
try:
    styled_df = df.style.apply(highlight_invalid_rows, axis=1);
    styled_df.to_excel(output_file, index=False, engine='openpyxl')
    print(f"{SYM_SUCCESS} Selesai! Hasil ID: {MY_PROCESS_ID} disimpan di {output_file}");
    print(f"{SYM_INFO} PERHATIAN: File ini hanya berisi data untuk jatah ID: {MY_PROCESS_ID}.");
    print(f"{SYM_INFO} Gabungkan dengan hasil chunk lain jika sudah semua.")
except Exception as e:
    print(f"{SYM_ERROR} Error simpan file Excel final ID: {MY_PROCESS_ID}: {e}");
    print(f"{SYM_WARNING} Mencoba simpan sbg CSV biasa...")
    try:
        csv_output_file = output_file.replace(".xlsx", "_plain.csv"); df.to_csv(csv_output_file, index=False); print(
            f"{SYM_SUCCESS} Berhasil simpan sbg CSV ke: {csv_output_file}")
    except Exception as e_csv:
        print(f"{SYM_ERROR} Gagal simpan sbg CSV juga: {e_csv}")

