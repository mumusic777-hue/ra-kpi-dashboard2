#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RA KPI Dashboard - KGI 売上・粗利実績 自動更新
================================================
毎日 0:00 に Claude Code スケジューラーから実行。

スプレッドシート（GID: 1083784169）の「個別メンバー成約金額」テーブルから
各メンバーの 実績金額 と 粗利【手動】 を取得し、
index.html の SAMPLE_DATA[今月].kgi を更新する。

スプレッドシート構成（GID: 1083784169）:
  ヘッダー行 : 担当CA | 月次目標成約数 | 月次目標金額 | 実績金額 | 達成状況 | 残予算 | 粗利【手動】
  データ行   : 森 / 浅沼 / 安木 / 山本

【事前準備】
  スプレッドシートを「リンクを知っている全員が閲覧可」に設定してください。
  （共有 → リンクを取得 → 閲覧者 に変更）
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

# Windows ターミナルの文字コードエラーを回避
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ('utf-8', 'utf8'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if sys.stderr.encoding and sys.stderr.encoding.lower() not in ('utf-8', 'utf8'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# =====================================================================
# 設定
# =====================================================================
SPREADSHEET_ID = "1pHIeBkQHlqhVzh875B8JlemMGSDk3cdhMgc2AB0gPJQ"
SHEET_GID      = "1083784169"

MEMBERS = ["森", "浅沼", "山本"]

HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
JST       = timezone(timedelta(hours=9))

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
                "スプレッドシートを「リンクを知っている全員が閲覧可」に\n"
                "設定してください。（共有 → リンクを取得 → 閲覧者）"
            ) from e
        raise RuntimeError(f"HTTP error {e.code}: {e.reason}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"URL error: {e.reason}") from e


# =====================================================================
# CSV から各メンバーの値を取得
# =====================================================================
def parse_member_values(csv_text: str) -> dict:
    """
    {
      "森":   {"売上実績": 10157830, "粗利実績": 2547181},
      "浅沼": {"売上実績": 900000,   "粗利実績": 270000},
      ...
    }
    を返す。
    """
    rows = list(csv.reader(io.StringIO(csv_text)))

    # ヘッダー行を探す（「担当CA」が含まれる行）
    header_row_idx = None
    for i, row in enumerate(rows):
        if any("担当CA" in cell for cell in row):
            header_row_idx = i
            break

    if header_row_idx is None:
        raise RuntimeError(
            "「担当CA」列が見つかりません。スプレッドシートの構造を確認してください。"
        )

    header = rows[header_row_idx]
    print(f"  ヘッダー行(idx={header_row_idx}): {header}")

    # 列インデックスを動的に取得
    col_member = next((i for i, h in enumerate(header) if "担当CA" in h), None)
    col_sales  = next((i for i, h in enumerate(header) if "実績金額" in h), None)
    col_profit = next((i for i, h in enumerate(header) if "粗利" in h and "手動" in h), None)

    if col_member is None:
        raise RuntimeError("「担当CA」列が見つかりません")
    if col_sales is None:
        raise RuntimeError("「実績金額」列が見つかりません")
    if col_profit is None:
        raise RuntimeError("「粗利【手動】」列が見つかりません")

    print(f"  列インデックス: 担当CA={col_member}, 実績金額={col_sales}, 粗利={col_profit}")

    def parse_amount(s: str) -> int:
        """「¥1,234,567」「-¥1,234,567」「1234567」などを int に変換"""
        cleaned = (
            s.strip()
             .replace("¥", "")
             .replace(",", "")
             .replace("，", "")
             .replace(" ", "")
             .replace("\u00a5", "")  # 全角¥
        )
        if not cleaned or cleaned == "-":
            return 0
        try:
            return int(float(cleaned))
        except ValueError:
            return 0

    result = {}
    for row in rows[header_row_idx + 1:]:
        if len(row) <= max(col_member, col_sales, col_profit):
            continue
        member = row[col_member].strip()
        if member not in MEMBERS:
            continue

        sales  = parse_amount(row[col_sales])
        profit = parse_amount(row[col_profit])

        result[member] = {"売上実績": sales, "粗利実績": profit}
        print(f"  📊 {member}: 売上実績={sales:,}, 粗利実績={profit:,}")

    return result


# =====================================================================
# 月キーを生成
# =====================================================================
def current_month_key() -> str:
    """例: '2026-03' """
    return datetime.now(JST).strftime("%Y-%m")


# =====================================================================
# index.html の KGI ブロックを更新
# =====================================================================
def update_kgi_in_html(content: str, ym: str, member_values: dict) -> tuple[str, bool]:
    """
    SAMPLE_DATA["YYYY-MM"].kgi の各メンバーの 粗利実績・売上実績 を更新する。

    対象パターン:
      森:   { 粗利実績:2547181, 粗利目標:1800000, 売上実績:10157830 },
    """
    # 月キーの位置を特定
    month_key_pos = content.find(f'"{ym}"')
    if month_key_pos == -1:
        return content, False

    # 次の月キー（"20XX-XX" 形式）の位置を探して範囲を絞る
    next_month_pos = content.find('"20', month_key_pos + len(f'"{ym}"'))
    if next_month_pos == -1:
        segment = content[month_key_pos:]
        before  = content[:month_key_pos]
        after   = ""
    else:
        segment = content[month_key_pos:next_month_pos]
        before  = content[:month_key_pos]
        after   = content[next_month_pos:]

    # kgi: ブロックが存在するか確認
    if "kgi:" not in segment:
        return content, False

    any_updated = False

    for member, vals in member_values.items():
        売上 = vals["売上実績"]
        粗利 = vals["粗利実績"]

        # パターン:
        # 森:   { 粗利実績:2547181, 粗利目標:1800000, 売上実績:10157830 },
        # グループ1: 「メンバー名:   { 粗利実績:」
        # グループ2: 「, 粗利目標:数値, 売上実績:」
        # グループ3: 「 }」
        pattern = (
            rf'({re.escape(member)}\s*:\s*\{{\s*粗利実績\s*:)\s*-?\d+'
            rf'(\s*,\s*粗利目標\s*:\s*-?\d+\s*,\s*売上実績\s*:)\s*-?\d+'
            rf'(\s*\}})'
        )
        new_segment, count = re.subn(
            pattern,
            rf'\g<1>{粗利}\g<2>{売上}\g<3>',
            segment,
            count=1
        )
        if count > 0:
            segment = new_segment
            any_updated = True
            print(f"  ✅ {ym} {member}: 粗利実績→{粗利:,}  売上実績→{売上:,}")
        else:
            print(f"  ⚠️  {ym} {member}: KGI行が見つかりません（パターン不一致）")

    if not any_updated:
        return content, False

    return before + segment + after, True


# =====================================================================
# data.json 書き込み（即時反映用）
# =====================================================================
DATA_JSON_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")

def write_data_json(ym: str, member_values: dict, now: datetime):
    """KGI データ（売上実績・粗利実績）を data.json に書き込む。"""
    try:
        with open(DATA_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = {}

    data.setdefault(ym, {})
    data[ym].setdefault("kgi", {})
    for member, vals in member_values.items():
        data[ym]["kgi"].setdefault(member, {})
        # 0 の場合は SAMPLE_DATA のフォールバック値を保持するためスキップ
        if vals["売上実績"] > 0:
            data[ym]["kgi"][member]["売上実績"] = vals["売上実績"]
        if vals["粗利実績"] > 0:
            data[ym]["kgi"][member]["粗利実績"] = vals["粗利実績"]

    data["lastUpdated"] = now.strftime("%Y-%m-%dT%H:%M:%S+09:00")
    with open(DATA_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  💾 data.json を更新しました（kgi: {ym}）")


def git_push(now: datetime):
    """data.json を git add → commit → push する。"""
    repo_dir = os.path.dirname(os.path.abspath(__file__))
    date_str  = now.strftime("%Y-%m-%d")
    msg       = f"auto: update kgi {date_str}"
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
# メイン更新処理
# =====================================================================
def update_html(member_values: dict):
    ym = current_month_key()

    with open(HTML_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    content, ok = update_kgi_in_html(content, ym, member_values)

    if not ok:
        print(f"  ⚠️  {ym} のKGIブロックが index.html に見つかりません")
        print(f"       HTML の kgi: {{ ... }} 構造を確認してください")
        return

    with open(HTML_PATH, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"  💾 index.html を保存しました")


# =====================================================================
# メイン
# =====================================================================
def main():
    now = datetime.now(JST)
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S JST')}] KGI 売上・粗利実績 更新開始")

    ym = current_month_key()
    print(f"  対象月: {ym}")

    print("  スプレッドシート取得中...")
    csv_text = fetch_csv()
    print(f"  取得完了（全{csv_text.count(chr(10))}行）")

    member_values = parse_member_values(csv_text)

    if not member_values:
        print("  ⚠️  メンバーデータが1件も取得できませんでした")
        return

    update_html(member_values)
    write_data_json(ym, member_values, now)
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
