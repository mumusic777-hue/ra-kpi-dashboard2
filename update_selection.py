#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RA KPI Dashboard - 選考パイプライン 自動更新
=============================================
毎日 0:00 に実行。

スプレッドシート (gid=1083784169):
  A列: 選考ステータス  B列: 進捗数
  例:
    書類選考数  135
    一次面接数  88
    二次面接数  1
    最終面接数  34
    内定数      13

【事前準備】
  スプレッドシートを「リンクを知っている全員が閲覧可」に設定してください。
"""

import sys, os, csv, io, re, json, urllib.request, urllib.error
from datetime import datetime, timezone, timedelta

if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8","utf8"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() not in ("utf-8","utf8"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# =====================================================================
# 設定
# =====================================================================
SPREADSHEET_ID = "1pHIeBkQHlqhVzh875B8JlemMGSDk3cdhMgc2AB0gPJQ"
GID            = "1083784169"

# スプシの行ラベル → index.html の selection キー
STATUS_MAP = {
    "書類選考数": "書類通過数",
    "一次面接数": "一次面接数",
    "二次面接数": "二次面接数",
    "最終面接数": "最終面接数",
    "内定数":     "内定数",
}

HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html")
JST       = timezone(timedelta(hours=9))

# =====================================================================
# CSV 取得
# =====================================================================
def fetch_csv() -> list:
    url = (
        f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
        f"/export?format=csv&gid={GID}"
    )
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (compatible; RA-KPI-Updater/1.0)"
    })
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return list(csv.reader(io.StringIO(resp.read().decode("utf-8"))))
    except urllib.error.HTTPError as e:
        if e.code == 401:
            raise RuntimeError(
                "スプレッドシートへのアクセスが拒否されました（401）。\n"
                "スプレッドシートを「リンクを知っている全員が閲覧可」に設定してください。"
            ) from e
        raise RuntimeError(f"HTTP error {e.code}: {e.reason}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"URL error: {e.reason}") from e


# =====================================================================
# 行ラベル → 進捗数 を辞書で返す
# =====================================================================
def parse_values(rows: list) -> dict:
    """
    A列のラベルが STATUS_MAP に含まれる行を探し、
    B列の数値を返す。
    """
    result = {}
    for row in rows:
        if len(row) < 3:
            continue
        label = row[1].strip()
        val_str = row[2].strip().replace(",", "").replace("，", "")
        if label not in STATUS_MAP:
            continue
        try:
            result[label] = int(float(val_str))
        except ValueError:
            print(f"  ⚠️  '{label}' の値 '{val_str}' を数値に変換できません")
    return result


# =====================================================================
# index.html 内の特定月の selection actual を更新
# =====================================================================
def update_selection_in_html(content: str, ym: str, key: str, new_val: int) -> tuple[str, bool]:
    """
    "YYYY-MM" セクション内の `key: { actual:N, target:T }` を更新する。
    """
    month_pos = content.find(f'"{ym}"')
    if month_pos == -1:
        return content, False

    # 次の月キーの手前までに絞る
    next_pos = content.find('"20', month_pos + len(f'"{ym}"'))
    if next_pos == -1:
        segment, before, after = content[month_pos:], content[:month_pos], ""
    else:
        segment, before, after = content[month_pos:next_pos], content[:month_pos], content[next_pos:]

    # key: { actual:N, target:T } の actual だけ置換
    pattern = rf'({re.escape(key)}:\s*\{{\s*actual\s*:)\s*\d+(\s*,\s*target\s*:)'
    new_seg, count = re.subn(pattern, rf'\g<1>{new_val}\g<2>', segment, count=1)
    if count == 0:
        return content, False
    return before + new_seg + after, True


# =====================================================================
# data.json 書き込み
# =====================================================================
DATA_JSON_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data.json")

def write_selection_data_json(ym: str, values: dict):
    """選考パイプラインデータを data.json に書き込む。"""
    try:
        with open(DATA_JSON_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        data = {}

    data.setdefault(ym, {})
    data[ym].setdefault("selection", {})
    for label, val in values.items():
        html_key = STATUS_MAP[label]
        data[ym]["selection"].setdefault(html_key, {"actual": 0, "target": 0})
        data[ym]["selection"][html_key]["actual"] = val

    with open(DATA_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  💾 data.json を更新しました（selection: {ym}）")


# =====================================================================
# メイン
# =====================================================================
def main():
    now   = datetime.now(JST)
    cur_ym = now.strftime("%Y-%m")
    print(f"[{now.strftime('%Y-%m-%d %H:%M:%S JST')}] 選考パイプライン 更新開始")
    print(f"  対象月: {cur_ym}")

    print("  スプレッドシート取得中...")
    rows = fetch_csv()
    print(f"  取得完了（全 {len(rows)} 行）")

    values = parse_values(rows)
    if not values:
        print("  ⚠️  対象データが見つかりませんでした。スプレッドシートの列A名称を確認してください。")
        print(f"     期待するラベル: {list(STATUS_MAP.keys())}")
        return

    print(f"  取得値: {values}")

    with open(HTML_PATH, "r", encoding="utf-8") as f:
        content = f.read()

    updated_any = False
    for label, val in values.items():
        html_key = STATUS_MAP[label]
        content, ok = update_selection_in_html(content, cur_ym, html_key, val)
        if ok:
            print(f"  ✅ {cur_ym} {html_key} actual → {val}")
            updated_any = True
        else:
            print(f"  ⚠️  {cur_ym} の '{html_key}' が index.html に見つかりません")

    if updated_any:
        with open(HTML_PATH, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"  💾 index.html を保存しました")
    else:
        print("  ⚠️  index.html の更新項目なし（新月度の場合は正常）")

    # data.json にも書き込む（HTML 更新の成否に関わらず）
    write_selection_data_json(cur_ym, values)

    print(f"[完了] {datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S JST')}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        import traceback; traceback.print_exc()
        sys.exit(1)
