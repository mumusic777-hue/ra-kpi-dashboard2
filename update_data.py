#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RA KPI Dashboard - AZ Data Updater
====================================
Claude Code スケジューラーから毎日実行。
Googleスプレッドシートの公開CSVをfetchし、
AZデータを集計して data.json に書き出す。

列構成:
  A(0): 求人ID       - 重複除外キー
  B(1): 登録日時     - 週・月の判定
  E(4): 求人担当     - カンマ区切りで複数名 / フルネームでマッチング
"""
import sys
import os
import csv
import io
import json
import re
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

# Windows ターミナルの文字コードエラーを回避
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ('utf-8', 'utf8'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if sys.stderr.encoding and sys.stderr.encoding.lower() not in ('utf-8', 'utf8'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# =====================================================================
# 設定（ここを変更してください）
# =====================================================================

SPREADSHEET_ID = "12cAUn9IbAB4ZoKHYgLSECiZ6YtBbF2-daj2VSo1aCrk"
SHEET_GID      = 0
HEADER_ROWS    = 1

# 列インデックス（0始まり）
COL_ID     = 0   # A: 求人ID
COL_DATE   = 1   # B: 登録日時
COL_MEMBER = 4   # E: 求人担当

# メンバー表示名 → スプレッドシート内のフルネーム マッピング
# フルネームを変更・追加したい場合はここだけ編集
MEMBER_NAMES = {
    "森":   "森雄大",
    "浅沼": "浅沼潤太",
    "山本": "山本涼介",
}

JST         = timezone(timedelta(hours=9))
OUTPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")

# =====================================================================
# CSV 取得
# =====================================================================
def fetch_csv() -> str:
    url = (
        f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
        f"/export?format=csv&gid={SHEET_GID}"
    )
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; RA-KPI-Updater/1.0)"
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"HTTP error {e.code}: {e.reason}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"URL error: {e.reason}") from e


# =====================================================================
# E列から担当メンバーを特定
# =====================================================================
# "サポート事務, 森雄大〈050-..〉, 浅沼潤太〈...〉" のような文字列から
# MEMBER_NAMES の value にマッチするメンバー（display名）のリストを返す
def extract_members(field: str) -> list:
    # カンマで分割し、各トークンから〈〉と電話番号を除去
    tokens = [t.strip() for t in field.split(",")]
    found = []
    for token in tokens:
        # 〈〉以降を削除（電話番号等）
        clean = re.split(r"[〈《\uff08\u3008]", token)[0].strip()
        # スペースを正規化
        clean = re.sub(r"\s+", " ", clean).strip()
        # フルネームとの完全一致チェック
        for display, fullname in MEMBER_NAMES.items():
            if clean == fullname:
                if display not in found:
                    found.append(display)
    return found


# =====================================================================
# 日付パース（複数フォーマット対応）
# =====================================================================
DATE_FORMATS = [
    "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d",
    "%Y-%m-%d",
]

def parse_date(s: str):
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s.strip(), fmt).replace(tzinfo=JST)
        except ValueError:
            continue
    return None


# =====================================================================
# KPI 月・週番号（最終金曜ルール）
# 月の最終週 = 最後の金曜日を含む週（月〜日）
# その翌月曜から次月の第1週
# =====================================================================
import calendar as _cal

def _last_friday(y: int, m: int) -> int:
    """月の最後の金曜日の日を返す"""
    last_day = _cal.monthrange(y, m)[1]
    for d in range(last_day, 0, -1):
        if datetime(y, m, d).weekday() == 4:  # 4=金曜
            return d
    return last_day

def _kpi_month_start(y: int, m: int) -> datetime:
    """KPI月の開始日（月曜）を返す。前月の最終金曜の週末日曜の翌日。"""
    pm = 12 if m == 1 else m - 1
    py = y - 1 if m == 1 else y
    plf_day = _last_friday(py, pm)
    plf = datetime(py, pm, plf_day, tzinfo=JST)
    days_to_sun = (6 - plf.weekday()) % 7  # weekday: 月=0..日=6
    pl_sun = plf + timedelta(days=days_to_sun)
    return pl_sun + timedelta(days=1)  # 翌月曜

def get_kpi_month_and_week(dt: datetime) -> tuple:
    """日付から (ym_string, week_number) を返す。"""
    # この日の属する週の金曜日を求める
    wd = dt.weekday()  # 月=0..日=6
    days_to_fri = 4 - wd  # 負値=過去方向（土日は同週の金曜を参照）
    friday = dt + timedelta(days=days_to_fri)
    # 金曜日のカレンダー月がKPI月
    kpi_y, kpi_m = friday.year, friday.month
    ym = f"{kpi_y:04d}-{kpi_m:02d}"
    # KPI月の開始月曜から何週目か
    start = _kpi_month_start(kpi_y, kpi_m)
    # dt の週の月曜
    mon_of_dt = dt - timedelta(days=wd)
    week_num = ((mon_of_dt - start).days // 7) + 1
    return ym, max(1, week_num)


# =====================================================================
# AZ 集計
# =====================================================================
def build_az(csv_text: str) -> dict:
    reader  = csv.reader(io.StringIO(csv_text))
    _header = next(reader, None)   # ヘッダー行スキップ

    seen      = set()   # 重複除外（求人ID）
    az        = {}      # { "YYYY-MM": { week(int): { member: count } } }
    skipped   = 0
    processed = 0

    for row_num, row in enumerate(reader, start=2):
        if len(row) <= max(COL_ID, COL_DATE, COL_MEMBER):
            skipped += 1
            continue

        job_id = row[COL_ID].strip()
        date_s = row[COL_DATE].strip()
        field  = row[COL_MEMBER].strip()

        if not job_id:
            skipped += 1
            continue

        # 重複除外
        if job_id in seen:
            continue
        seen.add(job_id)

        # メンバー特定
        members = extract_members(field)
        if not members:
            skipped += 1
            continue

        # 日付パース
        dt = parse_date(date_s)
        if dt is None:
            skipped += 1
            continue

        ym, week = get_kpi_month_and_week(dt)

        # 集計（1求人に複数担当がいる場合は全員にカウント）
        for m in members:
            az.setdefault(ym, {}).setdefault(week, {})
            az[ym][week][m] = az[ym][week].get(m, 0) + 1
            processed += 1

    print(f"  unique IDs: {len(seen)}, processed: {processed}, skipped: {skipped}")
    return az


# =====================================================================
# data.json 書き出し
# =====================================================================
def write_json(az: dict):
    # JSON は文字列キーのみ使用
    az_str = {
        ym: {str(w): members for w, members in sorted(weeks.items())}
        for ym, weeks in sorted(az.items())
    }

    # 既存データを読み込んで保持（p1, kgi 等の構造化データを消さない）
    try:
        with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        payload = {}

    # az 関連キーのみ更新
    payload["az"]          = az_str
    payload["lastUpdated"] = datetime.now(JST).isoformat()
    payload["source"]      = "Google Spreadsheet (Claude Code scheduled)"
    payload["members"]     = list(MEMBER_NAMES.keys())

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"  Written: {OUTPUT_PATH}")


# =====================================================================
# メイン
# =====================================================================
def main():
    now = datetime.now(JST)
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S JST')}] AZ data update start")

    print("  Fetching spreadsheet...")
    csv_text = fetch_csv()
    lines = csv_text.count("\n")
    print(f"  Fetched: ~{lines} rows")

    print("  Aggregating...")
    az = build_az(csv_text)

    # サマリー表示
    months = sorted(az.keys())
    print(f"  Months: {months}")
    for ym in months:
        for week in sorted(az[ym].keys()):
            members_data = az[ym][week]
            total = sum(members_data.values())
            detail = "  ".join(
                f"{m}:{members_data.get(m, 0)}"
                for m in MEMBER_NAMES.keys()
            )
            print(f"    {ym} Week{week}: total={total}  ({detail})")

    write_json(az)
    print("[Done] data.json updated successfully.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
