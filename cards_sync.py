# cards_sync.py
# Выгрузка полного перечня карточек WB (с размерами, баркодами, артикулами продавца)
# Content API v2: POST /content/v2/get/cards/list (курсорная пагинация)
#
# Требуемые ENV:
#   SUPABASE_URL
#   SUPABASE_SERVICE_ROLE
#   WB_API_KEY                  # ключ категории Content (или Promotion) из ЛК WB
# Опциональные ENV:
#   CURSOR_LIMIT=100            # лимит на страницу (1..100)
#   LOCALE=ru                   # ru|en|zh для полей name/value/object
#   SLEEP_BETWEEN_REQ_MS=700    # пауза между запросами (мс), чтобы не упираться в лимиты
#   BATCH_SIZE=1000             # размер батча апсерта в Supabase

import os
import time
from typing import Any, Dict, List, Optional, Iterable

import requests
from supabase import create_client, Client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE"]
WB_API_KEY   = os.environ["WB_API_KEY"]  # см. комментарий выше

CONTENT_V2_URL = "https://content-api.wildberries.ru/content/v2/get/cards/list"

CURSOR_LIMIT = max(1, min(int(os.environ.get("CURSOR_LIMIT", "100")), 100))
LOCALE = os.environ.get("LOCALE", None)  # None -> не добавлять параметр
SLEEP_BETWEEN_REQ_MS = int(os.environ.get("SLEEP_BETWEEN_REQ_MS", "700"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))

def _auth_headers() -> Dict[str, str]:
    return {"Authorization": WB_API_KEY, "Content-Type": "application/json"}

def _retryable_post(url: str, json: Dict[str, Any], params: Optional[Dict[str, Any]] = None) -> requests.Response:
    # Ретраим 429/5xx с бэкофом
    last_err: Optional[Exception] = None
    for attempt in range(1, 5):
        try:
            resp = requests.post(url, headers=_auth_headers(), json=json, params=params, timeout=120)
            if resp.status_code in (429, 500, 502, 503, 504):
                delay = min(30, 2 ** attempt)
                print(f"POST {url} -> {resp.status_code}, retry in {delay}s (attempt {attempt}/4)")
                time.sleep(delay)
                continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            last_err = e
            delay = min(30, 2 ** attempt)
            print(f"POST {url} error: {e!r}, retry in {delay}s (attempt {attempt}/4)")
            time.sleep(delay)
    raise RuntimeError(f"HTTP failed after retries: POST {url} last_err={last_err!r}")

def _flatten_cards(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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

def fetch_all_cards_v2() -> List[Dict[str, Any]]:
    """
    Курсорная пагинация (как в доке):
    1) Первый запрос: settings.cursor.limit=CURSOR_LIMIT, filter.withPhoto=-1 (все)
    2) На каждой странице берём cursor.updatedAt и cursor.nmID -> в следующий запрос
    3) Идём пока cursor.total >= limit (или пока приходят карточки)
    """
    all_flat: List[Dict[str, Any]] = []

    cursor_updated: Optional[str] = None
    cursor_nm: Optional[int] = None

    page = 0
    while True:
        page += 1
        cursor_obj: Dict[str, Any] = {"limit": CURSOR_LIMIT}
        if cursor_updated is not None and cursor_nm is not None:
            cursor_obj["updatedAt"] = cursor_updated
            cursor_obj["nmID"] = cursor_nm

        payload = {
            "settings": {
                "cursor": cursor_obj,
                "filter": {
                    "withPhoto": -1  # -1 — без фильтра по фото (все)
                }
            }
        }

        params = {"locale": LOCALE} if LOCALE else None
        resp = _retryable_post(CONTENT_V2_URL, json=payload, params=params)
        js = resp.json()

        cards = js.get("cards") or js.get("data") or []
        cursor = js.get("cursor") or {}

        total = cursor.get("total")
        updatedAt_next = cursor.get("updatedAt")
        nmID_next = cursor.get("nmID")

        # Расплющиваем
        flat = _flatten_cards(cards)
        all_flat.extend(flat)

        print(f"page {page}: cards={len(cards)}, flat={len(flat)}, total_flat={len(all_flat)}, "
              f"cursor.total={total}, next=({updatedAt_next},{nmID_next})")

        # Готовим курсор на следующую страницу
        cursor_updated = updatedAt_next
        cursor_nm = nmID_next

        # Условия выхода:
        # - если на странице нет карточек -> конец
        # - если cursor.total < limit -> конец (дока WB)
        if not cards:
            break
        if isinstance(total, int) and total < CURSOR_LIMIT:
            break

        # щадим лимиты: 100 req/min -> ~1 req/0.6s
        time.sleep(SLEEP_BETWEEN_REQ_MS / 1000.0)

    return all_flat

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
        res = sb.table("wb_products_catalog").upsert(
            batch, on_conflict="nm_id,barcode"
        ).execute()
        if getattr(res, "error", None):
            msg = getattr(res.error, "message", str(res.error))
            raise RuntimeError(f"Upsert error: {msg}")
        affected = len(res.data) if res.data is not None else 0
        sent += len(batch)
        print(f"Upserted {len(batch)} (affected≈{affected}), progress {sent}/{total}")

def main():
    rows = fetch_all_cards_v2()
    cleaned = [r for r in rows if r.get("nm_id") and r.get("barcode")]
    dropped = len(rows) - len(cleaned)
    if dropped:
        print(f"Dropped {dropped} rows without nm_id/barcode")

    sb: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    upsert_catalog(sb, cleaned)
    print(f"Done. Total catalog rows processed: {len(cleaned)}")

if __name__ == "__main__":
    main()
