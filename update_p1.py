#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RA KPI Dashboard - パイプライン① 新規開拓 自動更新
====================================================
毎日 0:00 に Claude Code スケジューラーから実行。

スプレッドシート: 1rze7tkzjaXqakGTsMix-F5mnKbQThB00OnkPvdtsLww (GID: 0)

カウント対象列:
  B列 (index 1) : 流入日時【アポ獲得日】 → アポイント数
  H列 (index 7) : 担当者                  → メンバー特定 (森/浅沼/安木/山本)
  U列 (index 20): 初回商談日              → 商談数
  X列 (index 23): 契約締結日              → 契約締結数
  Y列 (index 24): 現在のステータス        → 「既存対応」は全項目から除外

週の定義 (index.html の weekDateRange() に合わせる):
  月の最初の月曜を起点に月曜〜日曜で区切る。
  月初が月曜でない場合、1日〜最初の月曜前日は第1週に含める。
  月によって第5週が発生する場合がある。

【事前準備】
  スプレッドシートを「リンクを知っている全員が閲覧可」に設定してください。
"""

import sys
import os
import csv
import io
import re
import json
import subprocess
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
import calendar

# Windows ターミナルの文字コードエラーを回避
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ('utf-8', 'utf8'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if sys.stderr.encoding and sys.stderr.encoding.lower() not in ('utf-8', 'utf8'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# =====================================================================
# 設定
# =====================================================================
SPREADSHEET_ID = "1rze7tkzjaXqakGTsMix-F5mnKbQThB00OnkPvdtsLww"
SHEET_GID      = "0"

COL_APO        = 1   # B列: 流入日時（アポ獲得日）
COL_MEMBER     = 7   # H列: 担当者
COL_SHADAN     = 20  # U列: 初回商談日
COL_KEIYAKU    = 23  # X列: 契約締結日
COL_STATUS     = 24  # Y列: 現在のステータス

MEMBERS        = ["森", "浅沼", "山本"]
EXCLUDE_STATUS = "既存対応"

HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
JST       = timezone(timedelta(hours=9))

# 週の日付範囲定義（参考・使用は get_week_ranges() を参照）
# 月曜〜日曜で区切る。月によって5週になる場合がある。
WEEK_RANGES = {
    1: (1, 7),   # ← 旧定義（参考）
    2: (8, 14),
    3: (15, 21),
    # 第4週は22日〜月末（動的に計算）
}


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
        if e.code == 401:
            raise RuntimeError(
                "スプレッドシートへのアクセスが拒否されました（401）。\n"
                "「リンクを知っている全員が閲覧可」に設定してください。"
            ) from e
        raise RuntimeError(f"HTTP error {e.code}: {e.reason}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"URL error: {e.reason}") from e


# =====================================================================
# 日付文字列パース
# =====================================================================
def parse_date(raw: str):
    """
    各種フォーマットの日付文字列を datetime に変換。
    失敗時は None を返す。
    対応フォーマット例:
      2026/3/24 17:58
      2026/3/24
      2026-03-24
      2026-03-24 17:58:00
    """
    raw = raw.strip()
    if not raw:
        return None
    for fmt in (
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


# =====================================================================
# KPI 月・週番号（最終金曜ルール）
# 月の最終週 = 最後の金曜日を含む週（月〜日）
# その翌月曜から次月の第1週
# =====================================================================
def _last_friday(y: int, m: int) -> int:
    """月の最後の金曜日の日を返す"""
    last_day = calendar.monthrange(y, m)[1]
    for d in range(last_day, 0, -1):
        if datetime(y, m, d).weekday() == 4:  # 4=金曜
            return d
    return last_day

def _kpi_month_start(y: int, m: int) -> datetime:
    """KPI月の開始日（月曜）を返す"""
    pm = 12 if m == 1 else m - 1
    py = y - 1 if m == 1 else y
    plf_day = _last_friday(py, pm)
    plf = datetime(py, pm, plf_day)
    days_to_sun = (6 - plf.weekday()) % 7
    pl_sun = plf + timedelta(days=days_to_sun)
    return pl_sun + timedelta(days=1)

def get_kpi_month_and_week(dt: datetime) -> tuple:
    """日付から (ym_string, week_number) を返す"""
    wd = dt.weekday()  # 月=0..日=6
    days_to_fri = 4 - wd  # 負値=過去方向（土日は同週の金曜を参照）
    friday = dt + timedelta(days=days_to_fri)
    kpi_y, kpi_m = friday.year, friday.month
    ym = f"{kpi_y:04d}-{kpi_m:02d}"
    start = _kpi_month_start(kpi_y, kpi_m)
    mon_of_dt = dt - timedelta(days=wd)
    week_num = ((mon_of_dt.replace(tzinfo=None) - start.replace(tzinfo=None)).days // 7) + 1
    return ym, max(1, week_num)

def get_kpi_week_count(year: int, month: int) -> int:
    """KPI月の週数を返す"""
    start = _kpi_month_start(year, month)
    lf_day = _last_friday(year, month)
    lf = datetime(year, month, lf_day)
    days_to_sun = (6 - lf.weekday()) % 7
    last_sun = lf + timedelta(days=days_to_sun)
    return ((last_sun - start.replace(tzinfo=None)).days // 7) + 1


# =====================================================================
# CSVを集計
# =====================================================================
def aggregate(csv_text: str, year: int, month: int) -> dict:
    """
    KPI月の最終金曜ルールで集計。日付が属するKPI月・週を判定し、
    対象月のデータのみ返す。
    """
    target_ym = f"{year:04d}-{month:02d}"
    week_count = get_kpi_week_count(year, month)

    # 結果を 0 初期化
    result = {}
    for wn in range(1, week_count + 1):
        result[wn] = {
            "アポイント数":  {m: 0 for m in MEMBERS},
            "商談数":        {m: 0 for m in MEMBERS},
            "契約締結数":    {m: 0 for m in MEMBERS},
        }

    rows = list(csv.reader(io.StringIO(csv_text)))
    if not rows:
        return result

    data_rows = rows[1:]
    skipped = 0
    counted = 0

    for row_idx, row in enumerate(data_rows, start=2):
        if len(row) <= max(COL_APO, COL_MEMBER, COL_SHADAN, COL_KEIYAKU, COL_STATUS):
            continue

        member = row[COL_MEMBER].strip()
        if member not in MEMBERS:
            continue

        status = row[COL_STATUS].strip()
        if status == EXCLUDE_STATUS:
            skipped += 1
            continue

        counted += 1

        # ── アポイント数 ──
        apo_raw = row[COL_APO].strip() if len(row) > COL_APO else ""
        if apo_raw:
            dt = parse_date(apo_raw)
            if dt:
                ym, wn = get_kpi_month_and_week(dt)
                if ym == target_ym and wn in result:
                    result[wn]["アポイント数"][member] += 1

        # ── 商談数 ──
        shadan_raw = row[COL_SHADAN].strip() if len(row) > COL_SHADAN else ""
        if shadan_raw:
            dt = parse_date(shadan_raw)
            if dt:
                ym, wn = get_kpi_month_and_week(dt)
                if ym == target_ym and wn in result:
                    result[wn]["商談数"][member] += 1

        # ── 契約締結数 ──
        keiyaku_raw = row[COL_KEIYAKU].strip() if len(row) > COL_KEIYAKU else ""
        if keiyaku_raw:
            dt = parse_date(keiyaku_raw)
            if dt:
                ym, wn = get_kpi_month_and_week(dt)
                if ym == target_ym and wn in result:
                    result[wn]["契約締結数"][member] += 1

    print(f"  処理行数: {counted}行（既存対応除外: {skipped}行）")
    return result


# =====================================================================
# HTML 更新: 月・週・フィールド を指定して値を置換
# =====================================================================
def get_month_segment(content: str, ym: str):
    """
    (month_before, month_seg, month_after) を返す。
    """
    month_pos = content.find(f'"{ym}"')
    if month_pos == -1:
        return None

    next_m = re.search(r'"20\d\d-\d\d"', content[month_pos + 8:])
    if next_m:
        month_end = month_pos + 8 + next_m.start()
    else:
        month_end = len(content)

    return content[:month_pos], content[month_pos:month_end], content[month_end:]


def get_week_segment(month_seg: str, week_num: int):
    """
    月セグメント内から特定週ブロックを brace counting で抽出。
    (before, week_seg, after) を返す。
    """
    week_key_pat = rf'\b{week_num}\s*:\s*\{{'
    m = re.search(week_key_pat, month_seg)
    if not m:
        return None

    brace_start = m.end() - 1  # '{' の位置
    depth = 0
    i = brace_start
    while i < len(month_seg):
        if month_seg[i] == '{':
            depth += 1
        elif month_seg[i] == '}':
            depth -= 1
            if depth == 0:
                week_end = i + 1
                break
        i += 1
    else:
        return None

    return month_seg[:m.start()], month_seg[m.start():week_end], month_seg[week_end:]


def replace_p1_field(week_seg: str, field: str, vals: dict) -> tuple[str, bool]:
    """
    week_seg 内の p1 フィールド（アポイント数/商談数/契約締結数）を更新。
    MEMBERS リストから動的にパターンを生成。
    """
    # パターン: フィールド名: { メンバー1:N, メンバー2:N, ... }
    member_pattern = r'(\s*,\s*)'.join(
        rf'{re.escape(m)}\s*:\s*\d+' for m in MEMBERS
    )
    pattern = rf'({re.escape(field)}\s*:\s*\{{\s*)' + member_pattern + r'(\s*\})'

    # 置換文字列を動的に生成
    parts = [r'\g<1>']
    for i, m in enumerate(MEMBERS):
        if i > 0:
            parts.append(rf'\g<{i + 1}>')
        parts.append(f'{m}:{vals.get(m, 0)}')
    parts.append(rf'\g<{len(MEMBERS) + 1}>')
    replacement = ''.join(parts)

    new_seg, count = re.subn(pattern, replacement, week_seg, count=1)
    return new_seg, count > 0


def update_html(content: str, ym: str, aggregated: dict) -> tuple[str, int]:
    """
    index.html を更新して (新しいcontent, 更新件数) を返す。
    """
    segs = get_month_segment(content, ym)
    if segs is None:
        print(f"  ⚠️  '{ym}' が index.html に見つかりません")
        return content, 0

    month_before, month_seg, month_after = segs
    total_updates = 0

    for week_num in range(1, 5):
        week_segs = get_week_segment(month_seg, week_num)
        if week_segs is None:
            print(f"  ⚠️  第{week_num}週ブロックが見つかりません")
            continue

        w_before, week_seg, w_after = week_segs
        week_data = aggregated.get(week_num, {})
        week_changed = False

        for field in ("アポイント数", "商談数", "契約締結数"):
            vals = week_data.get(field, {m: 0 for m in MEMBERS})
            new_week_seg, ok = replace_p1_field(week_seg, field, vals)
            if ok:
                week_seg = new_week_seg
                week_changed = True
                total_updates += 1
                member_str = " ".join(f"{m}={vals.get(m, 0)}" for m in MEMBERS)
                print(f"  ✅ 第{week_num}週 {field}: {member_str}")
            else:
                print(f"  ⚠️  第{week_num}週 {field} のパターンが見つかりませんでした")

        if week_changed:
            month_seg = w_before + week_seg + w_after

    return month_before + month_seg + month_after, total_updates


# =====================================================================
# data.json 書き込み（即時反映用）
# =====================================================================
def write_data_json(ym: str, aggregated: dict, now: datetime):
    """
    p1 データ（アポイント数/商談数/契約締結数）を data.json に書き込む。
    HTML は変更せず、data.json のみ更新することで GitHub Raw 経由の即時反映を実現。
    """
    data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")
    try:
        with open(data_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = {}

    data.setdefault(ym, {})
    data[ym].setdefault("weeks", {})
    for wn, week_data in aggregated.items():
        # データが全員0の週はスキップ（空週をdata.jsonに作らない）
        has_data = any(
            week_data[f].get(m, 0) > 0
            for f in ("アポイント数", "商談数", "契約締結数")
            for m in MEMBERS
        )
        if not has_data:
            continue
        wn_str = str(wn)
        data[ym]["weeks"].setdefault(wn_str, {})
        data[ym]["weeks"][wn_str].setdefault("p1", {})
        for field in ("アポイント数", "商談数", "契約締結数"):
            data[ym]["weeks"][wn_str]["p1"][field] = week_data[field]

    data["lastUpdated"] = now.strftime("%Y-%m-%dT%H:%M:%S+09:00")
    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  💾 data.json を更新しました（p1: {ym}）")


# =====================================================================
# GitHub へ自動プッシュ
# =====================================================================
def git_push(now: datetime):
    """data.json を git add → commit → push する。失敗してもスクリプトは続行。"""
    repo_dir = os.path.dirname(os.path.abspath(__file__))
    date_str  = now.strftime("%Y-%m-%d")
    msg       = f"auto: update p1 pipeline {date_str}"
    try:
        subprocess.run(["git", "-C", repo_dir, "add", "data.json"],
                       check=True, capture_output=True)
        result = subprocess.run(["git", "-C", repo_dir, "diff", "--cached", "--quiet"],
                                capture_output=True)
        if result.returncode == 0:
            print("  ℹ️  data.json に変更なし。git push をスキップしました")
            return
        subprocess.run(["git", "-C", repo_dir, "commit", "-m", msg],
                       check=True, capture_output=True)
        subprocess.run(["git", "-C", repo_dir, "push"],
                       check=True, capture_output=True)
        print(f"  🚀 GitHub へ push 完了: {msg}")
    except subprocess.CalledProcessError as e:
        print(f"  ⚠️  git push 失敗（ローカルは更新済み）: {e.stderr.decode(errors='replace').strip()}")


# =====================================================================
# メイン
# =====================================================================
def main():
    now = datetime.now(JST)
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S JST')}] パイプライン① 自動更新開始")

    year  = now.year
    month = now.month
    ym    = now.strftime("%Y-%m")
    print(f"  対象月: {ym}  ({year}年{month}月)")

    # CSV取得
    print("  スプレッドシート取得中...")
    csv_text = fetch_csv()
    line_count = csv_text.count('\n')
    print(f"  取得完了 ({line_count}行)")

    # 集計
    print("  集計中...")
    aggregated = aggregate(csv_text, year, month)

    # 集計結果サマリー表示
    for wn in sorted(aggregated.keys()):
        for field in ("アポイント数", "商談数", "契約締結数"):
            vals = aggregated[wn][field]
            total = sum(vals.values())
            if total > 0:
                print(f"  📊 第{wn}週 {field}: {vals}")

    # HTML更新
    print("  index.html 更新中...")
    with open(HTML_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    new_content, updates = update_html(content, ym, aggregated)

    if updates == 0:
        print("  ⚠️  index.html の更新項目なし（新月度の場合は正常）")
    else:
        with open(HTML_PATH, "w", encoding="utf-8") as f:
            f.write(new_content)
        print(f"  💾 index.html を保存しました（{updates}項目更新）")

    # data.json は HTML 更新の成否に関わらず書き込む
    write_data_json(ym, aggregated, now)
    git_push(now)
    print(f"[完了] {datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S JST')}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
