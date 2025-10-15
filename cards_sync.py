# cards_sync.py
# Полный перечень товаров из WB Content API: размеры, баркоды (skus), артикулы продавца.
# Пишем в Supabase таблицу wb_products_catalog (upsert).

import os
import time
from typing import Any, Dict, List, Optional, Iterable

import requests
from supabase import create_client, Client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE"]
WB_API_KEY   = os.environ["WB_API_KEY"]

# Endpoints
CONTENT_V2 = "https://content-api.wildberries.ru/content/v2/get/cards/list"
CONTENT_V1_CURSOR = "https://content-api.wildberries.ru/content/v1/cards/cursor/list"

BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))

def _auth_headers() -> Dict[str, str]:
    return {"Authorization": WB_API_KEY, "Content-Type": "application/json"}

def _retryable_request(method: str, url: str, **kwargs) -> requests.Response:
    last_err: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.request(method, url, timeout=120, **kwargs)
            # На 429/5xx — ретрай с бэкофом
            if resp.status_code in (429, 500, 502, 503, 504):
                delay = min(30, 2 ** attempt)
                print(f"{method} {url} -> {resp.status_code}, retry in {delay}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(delay)
                continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_err = e
            delay = min(30, 2 ** attempt)
            print(f"{method} {url} error: {e!r}, retry in {delay}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(delay)
    raise SystemExit(f"HTTP failed after retries: {method} {url} last_err={last_err!r}")

def _flatten_v2_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for it in items:
        nm_id = it.get("nmID") or it.get("nmId")
        imt_id = it.get("imtID") or it.get("imtId")
        vendor_code = it.get("vendorCode")
        brand = it.get("brand")
        subject = it.get("subjectName") or it.get("subject")
        updated_at = it.get("updatedAt")
        sizes = it.get("sizes") or []
        for s in sizes:
            tech_size = s.get("techSize") or s.get("techSizeName")
            size_id = s.get("sizeID") or s.get("sizeId")
            skus = s.get("skus") or []
            # В некоторых ответах skus — массив строк (баркоды)
            for bc in skus:
                if not bc:
                    continue
                rows.append({
                    "nm_id": nm_id,
                    "imt_id": imt_id,
                    "vendor_code": vendor_code,
                    "brand": brand,
                    "subject": subject,
                    "tech_size": tech_size,
                    "size_id": size_id,
                    "barcode": str(bc),
                    "updated_at_wb": updated_at,
                })
    return rows

def _flatten_v1_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # v1 структура очень близка: nmID, imtID, vendorCode, sizes[].skus[]
    return _flatten_v2_items(items)

def fetch_all_cards_v2() -> List[Dict[str, Any]]:
    """
    Пагинация v2: limit/offset. Некоторые аккаунты используют курсор; в базе доков встречаются обе модели.
    В большинстве случаев работает limit/offset. Идём до пустого ответа.
    """
    all_rows: List[Dict[str, Any]] = []
    offset = 0
    limit = 1000  # максимально возможное
    while True:
        payload = {
            # фильтр пустой = «все карточки»
            "filter": {},
            # блок настроек пагинации (варианты назв.: "settings" или "page" у разных ревизий API)
            "settings": {"limit": limit, "offset": offset}
        }
        resp = _retryable_request("POST", CONTENT_V2, headers=_auth_headers(), json=payload)
        js = resp.json()
        # ожидаем список карточек в "data" или сразу массив (разные ревизии)
        items = js.get("data") if isinstance(js, dict) else js
        if not items:
            break
        flat = _flatten_v2_items(items)
        all_rows.extend(flat)
        print(f"v2 page: items={len(items)}, flat={len(flat)}, total_flat={len(all_rows)}, offset={offset}")
        offset += limit
        # предохранитель от бесконечного цикла
        if len(items) < limit:
            break
    return all_rows

def fetch_all_cards_v1_cursor() -> List[Dict[str, Any]]:
    """
    Классическая пагинация курсором: updatedAt + nmID.
    Берём "очень раннюю" точку и листаем до конца.
    """
    all_rows: List[Dict[str, Any]] = []
    cursor_updated = "1970-01-01T00:00:00Z"
    cursor_nm = 0
    limit = 1000
    while True:
        payload = {
            "sort": {
                "cursor": {"limit": limit, "updatedAt": cursor_updated, "nmID": cursor_nm},
                "sortBy": "updateAt"
            },
            "filter": {}  # без фильтров = «все карточки»
        }
        resp = _retryable_request("POST", CONTENT_V1_CURSOR, headers=_auth_headers(), json=payload)
        js = resp.json()
        items = js.get("data") if isinstance(js, dict) else js
        if not items:
            break
        flat = _flatten_v1_items(items)
        all_rows.extend(flat)
        print(f"v1 page: items={len(items)}, flat={len(flat)}, total_flat={len(all_rows)}, "
              f"cursor=({cursor_updated},{cursor_nm})")
        # обновляем курсор на последний элемент страницы
        last = items[-1]
        cursor_updated = last.get("updatedAt") or cursor_updated
        cursor_nm = last.get("nmID") or last.get("nmId") or cursor_nm
        if len(items) < limit:
            break
    return all_rows

def chunked(iterable: Iterable[Any], size: int) -> Iterable[List[Any]]:
    buf: List[Any] = []
    for x in iterable:
        buf.append(x)
        if len(buf) == size:
            yield buf
            buf = []
    if buf:
        yield buf

def upsert_catalog(sb: Client, rows: List[Dict[str, Any]]) -> None:
    total = len(rows)
    if total == 0:
        print("No rows to upsert")
        return
    sent = 0
    for batch in chunked(rows, BATCH_SIZE):
        res = sb.table("wb_products_catalog") \
                .upsert(batch, on_conflict="nm_id,barcode") \
                .execute()
        if getattr(res, "error", None):
            msg = getattr(res.error, "message", str(res.error))
            raise SystemExit(f"Upsert error: {msg}")
        affected = len(res.data) if res.data is not None else 0
        sent += len(batch)
        print(f"Upserted {len(batch)} (affected≈{affected}), progress {sent}/{total}")

def main():
    # 1) пробуем v2
    try:
        print("Trying Content API v2…")
        rows = fetch_all_cards_v2()
        if not rows:
            print("v2 returned 0 rows, falling back to v1 cursor…")
            rows = fetch_all_cards_v1_cursor()
    except Exception as e:
        print(f"v2 failed: {e!r}. Falling back to v1 cursor…")
        rows = fetch_all_cards_v1_cursor()

    # чистим мусорные строки без ключевых полей
    cleaned = [r for r in rows if r.get("nm_id") and r.get("barcode")]
    dropped = len(rows) - len(cleaned)
    if dropped:
        print(f"Dropped {dropped} rows without nm_id/barcode")

    sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    upsert_catalog(sb, cleaned)
    print(f"Done. Total catalog rows processed: {len(cleaned)}")

if __name__ == "__main__":
    main()
