import csv
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable


TOTAL_TRADING_COST_RATE = 0.0023

SUMMARY_CSV_COLUMNS = [
    "날짜",
    "종목코드",
    "종목명",
    "매수가",
    "평균 매수 금액",
    "총 매수금액",
    "평균 매도 금액",
    "총 매도금액",
    "매도 횟수",
    "손익률",
    "손익금액",
]


def to_int(value: Any) -> int:
    try:
        return int(float(str(value).replace(",", "").replace("원", "").strip()))
    except (TypeError, ValueError):
        return 0


def make_trade_summary_row(row: Dict[str, Any]) -> Dict[str, Any]:
    buy_qty = to_int(row.get("매수 수량"))
    buy_price = to_int(row.get("평균 매수 금액")) or to_int(row.get("매수가"))
    buy_amount = to_int(row.get("매수체결금액")) or to_int(row.get("매수 금액"))
    if buy_amount <= 0:
        buy_amount = buy_price * buy_qty
    if buy_qty > 0 and buy_amount > 0:
        buy_price = int(round(buy_amount / buy_qty))
    sell_amount = to_int(row.get("매도 금액"))
    sell_qty = to_int(row.get("매도 수량"))
    sell_price = to_int(row.get("평균 매도 금액"))
    if sell_amount <= 0 and sell_price > 0:
        sell_amount = sell_price * sell_qty
    if sell_price <= 0 and sell_qty > 0 and sell_amount > 0:
        sell_price = int(round(sell_amount / sell_qty))
    sell_count = to_int(row.get("매도 횟수"))
    trading_cost = int(round(sell_amount * TOTAL_TRADING_COST_RATE))
    profit_amount = sell_amount - buy_amount - trading_cost
    profit_rate = (profit_amount / buy_amount * 100) if buy_amount else 0

    return {
        "날짜": row.get("날짜") or datetime.now().strftime("%Y-%m-%d"),
        "종목코드": row.get("종목코드", ""),
        "종목명": row.get("종목명", ""),
        "매수가": buy_price,
        "매수 수량": buy_qty,
        "평균 매수 금액": buy_price,
        "매수 금액": buy_amount,
        "총 매수금액": buy_amount,
        "매도 수량": sell_qty,
        "평균 매도 금액": sell_price,
        "매도 금액": sell_amount,
        "총 매도금액": sell_amount,
        "매도 횟수": sell_count,
        "거래비용": trading_cost,
        "손익금액": profit_amount,
        "손익률": round(profit_rate, 2),
    }


def to_csv_row(summary_row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "날짜": summary_row.get("날짜", ""),
        "종목코드": summary_row.get("종목코드", ""),
        "종목명": summary_row.get("종목명", ""),
        "매수가": summary_row.get("매수가", ""),
        "평균 매수 금액": summary_row.get("평균 매수 금액", ""),
        "총 매수금액": summary_row.get("총 매수금액", summary_row.get("매수 금액", 0)),
        "평균 매도 금액": summary_row.get("평균 매도 금액", ""),
        "총 매도금액": summary_row.get("총 매도금액", summary_row.get("매도 금액", 0)),
        "매도 횟수": summary_row.get("매도 횟수", 0),
        "손익률": summary_row.get("손익률", 0),
        "손익금액": summary_row.get("손익금액", 0),
    }


def append_trade_summaries_csv(csv_path: Path, summary_rows: Iterable[Dict[str, Any]]) -> None:
    rows = [to_csv_row(row) for row in summary_rows]
    if not rows:
        return

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if csv_path.exists() and csv_path.stat().st_size > 0:
        with csv_path.open("r", newline="", encoding="utf-8-sig") as file:
            reader = csv.DictReader(file)
            existing_rows = list(reader)
            if reader.fieldnames and reader.fieldnames != SUMMARY_CSV_COLUMNS:
                with csv_path.open("w", newline="", encoding="utf-8-sig") as rewrite_file:
                    writer = csv.DictWriter(rewrite_file, fieldnames=SUMMARY_CSV_COLUMNS)
                    writer.writeheader()
                    writer.writerows(to_csv_row(row) for row in existing_rows)

    write_header = not csv_path.exists() or csv_path.stat().st_size == 0
    with csv_path.open("a", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=SUMMARY_CSV_COLUMNS)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def format_korean_date(value: datetime) -> str:
    return f"{value.year}년 {value.month}월 {value.day}일"


def save_trade_logs_txt(log_dir: Path, log_date: datetime, reg_log: str, buy_log: str, sell_log: str) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    title = format_korean_date(log_date)
    path = log_dir / f"{title}.txt"
    content = "\n\n".join(
        [
            title,
            "[등록/감시 로그]\n" + (reg_log or "").strip(),
            "[매수 로그]\n" + (buy_log or "").strip(),
            "[매도 로그]\n" + (sell_log or "").strip(),
        ]
    )
    path.write_text(content + "\n", encoding="utf-8-sig")
    return path
