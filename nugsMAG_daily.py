#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NUGS MAG 신보 자동화 파이프라인
- GitHub Actions에서 매일 오후 10시 KST 실행
- Google Sheets 신규 행 → Notion DB 등록
- 장르 변경 감지 → Notion 자동 업데이트
"""

import sys
import os
import logging
import json
import time
from datetime import datetime, date

import gspread
from google.oauth2.service_account import Credentials
from notion_client import Client

# ── 설정 ──────────────────────────────────────────────────────────────────────

NOTION_TOKEN       = os.environ.get("NOTION_TOKEN", "")
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID", "")
SPREADSHEET_ID     = os.environ.get("SPREADSHEET_ID", "")
SHEET_NAME         = "신보목록"

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Google Sheets 컬럼명 → (Notion 프로퍼티명, 타입)
COLUMN_MAP = {
    "아티스트":                     ("아티스트",                          "multi_select"),
    "앨범명":                       ("앨범명",                            "title"),
    "유형":                         ("유형",                              "select"),
    "장르":                         ("장르",                              "multi_select"),
    "발매일자":                     ("발매일자",                          "date"),
    "유통사":                       ("유통사",                            "multi_select"),
    "기획사":                       ("기획사",                            "multi_select"),
    "인트로가 내 귀를 사로잡았는가?": ("인트로가 좋았는가?",               "checkbox"),
    "인트로 후~1분이 괜찮은가?":     ("인트로-1분 구간이 좋았는가?",       "checkbox"),
    "끝까지 좋았는가?":              ("곡의 끝까지 좋았는가?",             "checkbox"),
    "좋았지만 안타깝게도 못들어감":  ("곡이 좋았지만 아쉽게도 들지 못한 곡", "checkbox"),
    "신규 장르 인가?":               ("신규 장르인가?",                    "checkbox"),
}

NOTION_ID_COL = "notion_id"  # 동기화 추적용 컬럼 (시트에 자동 추가)

# ── 로깅 ──────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── 값 변환 헬퍼 ──────────────────────────────────────────────────────────────

def to_multi_select(value: str) -> list:
    return [{"name": v.strip()} for v in value.split(",") if v.strip()]

def to_checkbox(value: str) -> bool:
    return value.strip().upper() in ("TRUE", "1", "Y", "YES")

def normalize_date(value: str):
    for fmt in ("%Y-%m-%d", "%Y.%m.%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(value.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

def build_notion_properties(row: dict) -> dict:
    props = {}
    for sheet_col, (notion_col, col_type) in COLUMN_MAP.items():
        value = row.get(sheet_col, "").strip()
        if not value:
            continue
        if col_type == "title":
            props[notion_col] = {"title": [{"text": {"content": value}}]}
        elif col_type == "multi_select":
            props[notion_col] = {"multi_select": to_multi_select(value)}
        elif col_type == "select":
            props[notion_col] = {"select": {"name": value}}
        elif col_type == "date":
            d = normalize_date(value)
            if d:
                props[notion_col] = {"date": {"start": d}}
        elif col_type == "checkbox":
            props[notion_col] = {"checkbox": to_checkbox(value)}
    return props

# ── Google Sheets 연동 ────────────────────────────────────────────────────────

def get_sheet():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS", "")
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS 환경변수가 없습니다.")
    creds_dict = json.loads(creds_json)
    creds      = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    client     = gspread.authorize(creds)
    return client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

# ── Notion 연동 ───────────────────────────────────────────────────────────────

def get_notion_client() -> Client:
    if not NOTION_TOKEN:
        raise ValueError("NOTION_TOKEN 환경변수가 없습니다.")
    if not NOTION_DATABASE_ID:
        raise ValueError("NOTION_DATABASE_ID 환경변수가 없습니다.")
    return Client(auth=NOTION_TOKEN)

def create_notion_page(notion: Client, row: dict) -> str:
    artist      = row.get("아티스트", "")
    album_title = row.get("앨범명", "")
    copy_text   = f"{artist}-{album_title}"
    props       = build_notion_properties(row)

    page = notion.pages.create(
        parent={"database_id": NOTION_DATABASE_ID},
        properties=props,
        children=[
            {
                "object": "block",
                "type": "paragraph",
                "paragraph": {
                    "rich_text": [{"type": "text", "text": {"content": copy_text}}]
                },
            }
        ],
    )
    return page["id"]

def get_notion_genres(notion: Client, page_id: str) -> set:
    page = notion.pages.retrieve(page_id=page_id)
    return {
        tag["name"]
        for tag in page["properties"].get("장르", {}).get("multi_select", [])
    }

def update_notion_genre(notion: Client, page_id: str, genre_value: str):
    notion.pages.update(
        page_id=page_id,
        properties={"장르": {"multi_select": to_multi_select(genre_value)}},
    )

# ── 메인 ──────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 50)
    log.info(f"NUGS MAG Sheets → Notion 동기화 시작 — {date.today()}")
    log.info("=" * 50)

    sheet      = get_sheet()
    all_values = sheet.get_all_values()

    if not all_values:
        log.info("시트가 비어 있습니다.")
        return

    header    = all_values[0]
    data_rows = all_values[1:]

    # notion_id 트래킹 컬럼 확인 / 없으면 자동 추가
    if NOTION_ID_COL in header:
        notion_id_col_idx = header.index(NOTION_ID_COL)
    else:
        notion_id_col_idx = len(header)
        # 시트 컬럼 수 확장 후 헤더 추가
        sheet.add_cols(1)
        sheet.update_cell(1, notion_id_col_idx + 1, NOTION_ID_COL)
        header.append(NOTION_ID_COL)
        log.info(f"'{NOTION_ID_COL}' 컬럼 추가 (열 {notion_id_col_idx + 1})")

    notion          = get_notion_client()
    new_count       = 0
    updated_count   = 0
    pending_updates = []  # (row_idx, page_id) — 마지막에 일괄 기록

    for row_idx, row_values in enumerate(data_rows, start=2):
        # 행을 딕셔너리로 변환 (컬럼 수 부족 시 빈 문자열로 채움)
        row = {
            col: (row_values[i] if i < len(row_values) else "")
            for i, col in enumerate(header)
        }

        album_title = row.get("앨범명", "").strip()
        if not album_title:
            continue  # 빈 행 스킵

        notion_id = row.get(NOTION_ID_COL, "").strip()

        if not notion_id:
            # 신규 행 → Notion 등록 (notion_id는 나중에 일괄 기록)
            try:
                time.sleep(0.35)  # Notion API 초당 3건 제한 대응
                page_id = create_notion_page(notion, row)
                pending_updates.append((row_idx, page_id))
                log.info(f"  [신규] {album_title[:40]}")
                new_count += 1
            except Exception as e:
                log.error(f"  [신규 실패] {album_title}: {e}")

        else:
            # 기존 행 → 장르 변경 감지
            sheet_genre = row.get("장르", "").strip()
            if not sheet_genre:
                continue
            try:
                time.sleep(0.35)  # Notion API 초당 3건 제한 대응
                notion_genres = get_notion_genres(notion, notion_id)
                sheet_genres  = {v.strip() for v in sheet_genre.split(",") if v.strip()}

                if sheet_genres != notion_genres:
                    time.sleep(0.35)
                    update_notion_genre(notion, notion_id, sheet_genre)
                    log.info(
                        f"  [장르 수정] {album_title[:30]} | "
                        f"{notion_genres} → {sheet_genres}"
                    )
                    updated_count += 1
            except Exception as e:
                log.error(f"  [장르 확인 실패] {album_title}: {e}")

    # notion_id 일괄 기록 (Google Sheets API 호출 최소화)
    if pending_updates:
        log.info(f"notion_id {len(pending_updates)}개 시트에 일괄 기록 중...")
        col_letter = chr(ord('A') + notion_id_col_idx)
        sheet.batch_update([
            {
                "range": f"{col_letter}{row_idx}",
                "values": [[page_id]],
            }
            for row_idx, page_id in pending_updates
        ])
        log.info("일괄 기록 완료")

    log.info(f"완료 — 신규 {new_count}개 등록 / 장르 {updated_count}개 업데이트")

if __name__ == "__main__":
    main()
