import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from PyQt5 import uic
from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QApplication,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,   
    QTableWidget,
    QTableWidgetItem,
    QTextEdit,
)
try:
    from pykrx import stock
except Exception:
    stock = None

from watch_selection import DEMO_OPEN_PRICES, select_watch_candidates
from order_manager import BuySellOrderManager
from columns import (
    LIST0_COLUMNS,
    LIST0_MONEY_COLUMNS,
    LIST1_COLUMNS,
    LIST1_MONEY_COLUMNS,
    LIST2_COLUMNS,
    LIST2_MONEY_COLUMNS,
    LIST3_COLUMNS,
    LIST3_MONEY_COLUMNS,
    QTY_COLUMNS,
)
from state_store import load_state_file, save_state_file


def parse_int(text: str) -> int:
    cleaned = re.sub(r"[^\d\-]", "", text or "")
    if cleaned in {"", "-"}:
        return 0
    return int(cleaned)


def format_won(value: Any) -> str:
    try:
        num = int(float(value))
    except (TypeError, ValueError):
        num = 0
    return f"{num:,}원"


def format_number(value: Any) -> str:
    try:
        num = int(float(value))
    except (TypeError, ValueError):
        num = 0
    return f"{num:,}"


class PendingKiwoomOrderClient:
    def __init__(self, buy_log, sell_log) -> None:
        self.buy_log = buy_log
        self.sell_log = sell_log

    def send_buy_order(self, row: Dict[str, Any]) -> bool:
        name = row.get("종목명") or row.get("종목코드") or ""
        self.buy_log(f"{name}: 키움 API 미연결 - 매수 주문을 실행하지 않았습니다.")
        return False

    def cancel_buy_order(self, row: Dict[str, Any]) -> bool:
        name = row.get("종목명") or row.get("종목코드") or ""
        self.buy_log(f"{name}: 키움 API 미연결 - 매수 취소를 실행하지 않았습니다.")
        return False

    def send_sell_order(self, row: Dict[str, Any], price: int, qty: int) -> Optional[str]:
        name = row.get("종목명") or row.get("종목코드") or ""
        self.sell_log(f"{name}: 키움 API 미연결 - 매도 주문을 실행하지 않았습니다. ({price:,}원/{qty}주)")
        return None

    def cancel_sell_orders(self, row: Dict[str, Any]) -> bool:
        name = row.get("종목명") or row.get("종목코드") or ""
        self.sell_log(f"{name}: 키움 API 미연결 - 잔여 지정가 매도 취소를 실행하지 않았습니다.")
        return False

    def send_market_sell_order(self, row: Dict[str, Any], qty: int) -> Optional[str]:
        name = row.get("종목명") or row.get("종목코드") or ""
        self.sell_log(f"{name}: 키움 API 미연결 - 시장가 매도를 실행하지 않았습니다. ({qty}주)")
        return None

    def start_realtime_watch(self, row: Dict[str, Any]) -> None:
        name = row.get("종목명") or row.get("종목코드") or ""
        self.buy_log(f"{name}: 키움 API 미연결 - 실시간 감시를 시작하지 않았습니다.")

    def stop_realtime_watch(self, row: Dict[str, Any]) -> None:
        name = row.get("종목명") or row.get("종목코드") or ""
        self.buy_log(f"{name}: 키움 API 미연결 - 실시간 감시를 중단하지 않았습니다.")


class DMainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.base_dir = Path(__file__).resolve().parent
        self.state_path = self.base_dir / "D_Ver1_state.json"
        self._dirty = False

        ui_path = self._resolve_ui_path()
        uic.loadUi(str(ui_path), self)

        self.list0: List[Dict[str, Any]] = []
        self.list1: List[Dict[str, Any]] = []
        self.list2: List[Dict[str, Any]] = []
        self.list3: List[Dict[str, Any]] = []
        self._bind_widgets()
        self._init_tables()
        self._connect_signals()
        self.load_state()
        self.order_manager = self._create_order_manager()
        self.refresh_all_tables()

    def _resolve_ui_path(self) -> Path:
        candidates = [self.base_dir / "D_Ver1.1.ui", self.base_dir / "D_Ver1.ui", self.base_dir / "D_Ver1.0.ui"]
        for path in candidates:
            if path.exists():
                return path
        raise FileNotFoundError("D_Ver1.ui 또는 D_Ver1.0.ui 파일을 찾을 수 없습니다.")

    def _get(self, names: List[str], required: bool = True) -> Optional[Any]:
        for name in names:
            widget = self.findChild(object, name)
            if widget is not None:
                return widget
        if required:
            raise AttributeError(f"필수 위젯을 찾을 수 없습니다: {names}")
        return None

    def _bind_widgets(self) -> None:
        # 신규 D_Ver1.ui 이름 + 기존 D_Ver1.0.ui 이름 fallback
        self.lineEdit_code: Optional[QLineEdit] = self._get(["lineEdit_code", "lineEdit_name", "lineEdit"], required=False)
        self.lineEdit_Buy1: Optional[QLineEdit] = self._get(["lineEdit_Buy1", "lineEdit_2"], required=False)
        self.lineEdit_Cost: Optional[QLineEdit] = self._get(["lineEdit_Cost", "lineEdit_4"], required=False)
        self.lineEdit_Sell11: Optional[QLineEdit] = self._get(["lineEdit_Sell11", "lineEdit_5"], required=False)
        self.lineEdit_Sell12: Optional[QLineEdit] = self._get(["lineEdit_Sell12", "lineEdit_6"], required=False)
        self.lineEdit_Sell13: Optional[QLineEdit] = self._get(["lineEdit_Sell13", "lineEdit_7"], required=False)
        self.lineEdit_lose1: Optional[QLineEdit] = self._get(["lineEdit_lose1", "lineEdit_8"], required=False)
        self.lineEdit_rank: Optional[QLineEdit] = self._get(["lineEdit_rank", "lineEdit_9"], required=False)

        self.tableWidget_Book: Optional[QTableWidget] = self._get(["tableWidget_Book", "tableWidget_D0"], required=False)
        self.tableWidget_Buy: Optional[QTableWidget] = self._get(["tableWidget_Buy", "tableWidget_D0_Buy"], required=False)
        self.tableWidget_Sell: Optional[QTableWidget] = self._get(["tableWidget_Sell", "tableWidget_D0_Sell"], required=False)
        self.tableWidget_Summary: Optional[QTableWidget] = self._get(["tableWidget_Summary", "tableWidget_D0_Summary"], required=False)

        self.textEdit_Buy: Optional[QTextEdit] = self._get(["textEdit_Buy", "textEdit_D0"], required=False)
        self.textEdit_Sell: Optional[QTextEdit] = self._get(["textEdit_Sell", "textEdit_D0_Sell"], required=False)
        self.textEdit_Summary: Optional[QTextEdit] = self._get(["textEdit_Summary", "textEdit_D0_Summary"], required=False)
        self.textEdit_reg: Optional[QTextEdit] = self._get(["textEdit_reg"], required=False)

        self.pushButton_ini: Optional[QPushButton] = self._get(["pushButton_ini"], required=False)
        self.pushButton_reg: Optional[QPushButton] = self._get(["pushButton_reg", "pushButton_D0_1"], required=False)
        self.pushButton_reg_c: Optional[QPushButton] = self._get(["pushButton_reg_c", "pushButton_D0_2"], required=False)
        self.pushButton_Start: Optional[QPushButton] = self._get(["pushButton_Start"], required=False)
        self.pushButton_Stop: Optional[QPushButton] = self._get(["pushButton_Stop"], required=False)
        self.pushButton_demo: Optional[QPushButton] = self._get(["pushButton_demo"], required=False)
        self.pushButton_Save: Optional[QPushButton] = self._get(["pushButton_Save"], required=False)
        self.pushButton_End: Optional[QPushButton] = self._get(["pushButton_End"], required=False)

    def _init_tables(self) -> None:
        self._init_table(self.tableWidget_Book, LIST0_COLUMNS + ["삭제"])
        self._init_table(self.tableWidget_Buy, LIST1_COLUMNS + ["감시버튼"])
        self._init_table(self.tableWidget_Sell, LIST2_COLUMNS + ["감시버튼"])
        self._init_table(self.tableWidget_Summary, LIST3_COLUMNS + ["삭제"])

    def _init_table(self, table: Optional[QTableWidget], headers: List[str]) -> None:
        if table is None:
            return
        table.setColumnCount(len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setEditTriggers(QTableWidget.NoEditTriggers)
        table.setSelectionBehavior(QTableWidget.SelectRows)
        table.setSelectionMode(QTableWidget.SingleSelection)
        table.verticalHeader().setVisible(False)

    def _connect_signals(self) -> None:
        if self.pushButton_ini:
            self.pushButton_ini.clicked.connect(self.on_initialize)
        if self.pushButton_reg:
            self.pushButton_reg.clicked.connect(lambda: self.register_symbol("일반"))
        if self.pushButton_reg_c:
            self.pushButton_reg_c.clicked.connect(lambda: self.register_symbol("신용"))
        if self.pushButton_Start:
            self.pushButton_Start.clicked.connect(self.start_watch_all)
        if self.pushButton_Stop:
            self.pushButton_Stop.clicked.connect(self.stop_watch_all)
        if self.pushButton_demo:
            self.pushButton_demo.clicked.connect(self.run_demo_watch_selection)
        if self.pushButton_Save:
            self.pushButton_Save.clicked.connect(self.save_state)
        if self.pushButton_End:
            self.pushButton_End.clicked.connect(self.close)

        for edit in self.findChildren(QLineEdit):
            edit.textChanged.connect(self.mark_dirty)

    def _create_order_manager(self) -> BuySellOrderManager:
        return BuySellOrderManager(
            self.list1,
            self.list2,
            self.list3,
            PendingKiwoomOrderClient(self.append_buy_log, self.append_sell_log),
            self.append_buy_log,
            self.append_sell_log,
            self._refresh_after_order_manager_update,
        )

    def _refresh_after_order_manager_update(self) -> None:
        self.mark_dirty()
        self.refresh_all_tables()

    def mark_dirty(self) -> None:
        self._dirty = True

    def _line_int(self, edit: Optional[QLineEdit], default: int = 0) -> int:
        if edit is None:
            return default
        return parse_int(edit.text())

    def _line_text(self, edit: Optional[QLineEdit]) -> str:
        return edit.text().strip() if edit is not None else ""

    def _calc_buy_qty(self, cost: int, buy_price: int) -> int:
        if cost <= 0 or buy_price <= 0:
            return 0
        return int((cost / buy_price) + 0.5)

    def _normalize_code(self, raw: str) -> str:
        code = re.sub(r"\D", "", raw or "")
        return code.zfill(6) if code else ""

    def _parse_optional_sell(self, edit: Optional[QLineEdit]) -> Optional[int]:
        text = self._line_text(edit)
        if text == "":
            return None
        value = parse_int(text)
        if value == 0:
            return None
        return value

    def _fetch_name_and_close(self, code: str) -> Optional[Dict[str, Any]]:
        if stock is None:
            return None

        name = stock.get_market_ticker_name(code)
        if not name:
            return None

        now = datetime.now()
        today = now.date()
        start = (today - timedelta(days=45)).strftime("%Y%m%d")
        end = today.strftime("%Y%m%d")

        try:
            df = stock.get_market_ohlcv_by_date(start, end, code)
        except Exception:
            return None
        if df is None or df.empty:
            return None

        trade_dates = [idx.date() for idx in df.index]
        latest_trade_date = max(trade_dates)
        is_today_trade_day = today in trade_dates

        if is_today_trade_day and now.hour >= 18:
            ref_date = today
        elif is_today_trade_day:
            previous_days = [d for d in trade_dates if d < today]
            ref_date = max(previous_days) if previous_days else today
        else:
            ref_date = latest_trade_date

        ref_row = df.loc[df.index.date == ref_date].iloc[-1]
        close = int(ref_row["종가"])
        volume = int(ref_row["거래량"]) if "거래량" in ref_row.index else None
        return {"name": name, "close": close, "volume": volume}

    def register_symbol(self, buy_type: str) -> None:
        code = self._normalize_code(self._line_text(self.lineEdit_code))
        cost_text = self._line_text(self.lineEdit_Cost)
        buy1_text = self._line_text(self.lineEdit_Buy1)
        sell11_text = self._line_text(self.lineEdit_Sell11)
        lose1_text = self._line_text(self.lineEdit_lose1)
        if not code or not cost_text or not buy1_text or not sell11_text or not lose1_text:
            QMessageBox.warning(self, "입력 오류", "모두 입력해 주세요")
            return

        if any(item.get("종목코드") == code for item in self.list0):
            QMessageBox.warning(self, "중복 등록", "이미 등록된 종목 입니다")
            return

        cost = parse_int(cost_text)
        buy1 = parse_int(buy1_text)
        sell11 = parse_int(sell11_text)
        lose1 = parse_int(lose1_text)
        sell12 = self._parse_optional_sell(self.lineEdit_Sell12)
        sell13 = self._parse_optional_sell(self.lineEdit_Sell13)
        rank = self._line_int(self.lineEdit_rank)

        if cost <= 0 or buy1 <= 0 or sell11 <= 0 or lose1 <= 0:
            QMessageBox.warning(self, "입력 오류", "가격 입력 오류")
            return
        if sell12 is not None and sell12 < 0:
            QMessageBox.warning(self, "입력 오류", "가격 입력 오류")
            return
        if sell13 is not None and sell13 < 0:
            QMessageBox.warning(self, "입력 오류", "가격 입력 오류")
            return

        sell_prices = [sell11]
        if sell12 is not None:
            sell_prices.append(sell12)
        if sell13 is not None:
            sell_prices.append(sell13)
        if any(left >= right for left, right in zip(sell_prices, sell_prices[1:])):
            QMessageBox.warning(self, "입력 오류", "매도가 입력 오류")
            return

        market = self._fetch_name_and_close(code)
        if market is None:
            QMessageBox.warning(self, "입력 오류", "종목 코드 오류")
            return

        if buy1 > market["close"]:
            QMessageBox.warning(self, "등록 경고", "종가가 높습니다")
        if not (buy1 * 0.8 < lose1 < buy1):
            QMessageBox.warning(self, "등록 경고", "손절가 이상")

        buy_qty = self._calc_buy_qty(cost, buy1)
        row0 = {
            "종목코드": code,
            "종목명": market["name"],
            "전일 거래량": market["volume"],
            "기준 종가": market["close"],
            "시가": None,
            "현재가": None,
            "매수 금액": cost,
            "매수 수량": buy_qty,
            "제1 매수가": buy1,
            "매도가1-1": sell11,
            "매도가1-2": sell12,
            "매도가1-3": sell13,
            "제1 손절가": lose1,
            "매수 타입": buy_type,
        }
        self.list0.append(row0)

        self.mark_dirty()
        self.refresh_all_tables()

    def on_initialize(self) -> None:
        for edit in self.findChildren(QLineEdit):
            edit.clear()

    def start_watch_all(self) -> None:
        for row_idx in range(len(self.list1)):
            self.order_manager.start_buy_watch(row_idx)
        for row in self.list2:
            row["감시 상태"] = "감시 중"
            row.pop("감시상태", None)
        self.append_buy_log("[감시] 매수 감시를 시작했습니다.")
        self.append_sell_log("[감시] 매도 감시를 시작했습니다.")
        self.mark_dirty()
        self.refresh_all_tables()

    def stop_watch_all(self) -> None:
        for row_idx in range(len(self.list1)):
            self.order_manager.stop_buy_watch(row_idx)
        for row in self.list2:
            row["감시 상태"] = "감시 중지"
            row.pop("감시상태", None)
        self.append_buy_log("[감시] 매수 감시를 중지했습니다.")
        self.append_sell_log("[감시] 매도 감시를 중지했습니다.")
        self.mark_dirty()
        self.refresh_all_tables()

    def run_demo_watch_selection(self) -> None:
        if not self.list0:
            QMessageBox.information(self, "모의 테스트", "등록된 종목이 없습니다.")
            return

        priority_limit = self._line_int(self.lineEdit_rank)
        result = select_watch_candidates(self.list0, priority_limit, DEMO_OPEN_PRICES)

        if result.selected_rows:
            self.list1.extend(result.selected_rows)
            selected_codes = set(result.selected_codes)
            self.list0 = [
                row
                for row in self.list0
                if str(row.get("종목코드", "")) not in selected_codes
            ]

        if self.textEdit_reg:
            self.textEdit_reg.append("[모의 테스트] 종목 선별을 실행했습니다.")
            for message in result.stopped_messages:
                self.textEdit_reg.append(message)
            for row in result.selected_rows:
                name = row.get("종목명", row.get("종목코드", ""))
                rank = row.get("우선순위", "")
                self.textEdit_reg.append(f"{name} 감시 등록: 갭 상승 순위 {rank}")

        self.mark_dirty()
        self.refresh_all_tables()

    def append_buy_log(self, message: str) -> None:
        if self.textEdit_Buy:
            self.textEdit_Buy.append(message)

    def append_sell_log(self, message: str) -> None:
        if self.textEdit_Sell:
            self.textEdit_Sell.append(message)

    def refresh_all_tables(self) -> None:
        self._render_list0()
        self._render_list1()
        self._render_list2()
        self._render_list3()

    def _set_item(self, table: QTableWidget, row: int, col: int, value: str) -> None:
        item = QTableWidgetItem(value)
        item.setTextAlignment(Qt.AlignCenter)
        table.setItem(row, col, item)

    def _prepare_table_render(self, table: QTableWidget, row_count: int) -> None:
        for row_idx in range(table.rowCount()):
            for col_idx in range(table.columnCount()):
                if table.cellWidget(row_idx, col_idx) is not None:
                    table.removeCellWidget(row_idx, col_idx)
        table.clearContents()
        table.setRowCount(row_count)

    def _format_cell(self, column: str, value: Any, money_cols: set) -> str:
        if value in (None, ""):
            return "--"
        if column in money_cols:
            return format_won(value)
        if column in QTY_COLUMNS:
            return format_number(value)
        return str(value if value is not None else "")

    def _render_list0(self) -> None:
        table = self.tableWidget_Book
        if table is None:
            return
        self._prepare_table_render(table, len(self.list0))
        for row_idx, row in enumerate(self.list0):
            for col_idx, col_name in enumerate(LIST0_COLUMNS):
                self._set_item(table, row_idx, col_idx, self._format_cell(col_name, row.get(col_name), LIST0_MONEY_COLUMNS))
            delete_btn = QPushButton("삭제")
            delete_btn.clicked.connect(lambda _checked=False, code=row.get("종목코드"): self.delete_book_row_by_code(code))
            table.setCellWidget(row_idx, len(LIST0_COLUMNS), delete_btn)

        table.resizeColumnsToContents()

    def _render_list1(self) -> None:
        table = self.tableWidget_Buy
        if table is None:
            return
        self._prepare_table_render(table, len(self.list1))
        for row_idx, row in enumerate(self.list1):
            for col_idx, col_name in enumerate(LIST1_COLUMNS):
                self._set_item(table, row_idx, col_idx, self._format_cell(col_name, row.get(col_name), LIST1_MONEY_COLUMNS))

            watch = row.get("감시상태", "감시 중")
            watch_btn = QPushButton("감시 중지" if watch == "감시 중" else "감시 시작")
            watch_btn.clicked.connect(lambda _checked=False, idx=row_idx: self.toggle_buy_watch(idx))
            table.setCellWidget(row_idx, len(LIST1_COLUMNS), watch_btn)

        table.resizeColumnsToContents()

    def _render_list2(self) -> None:
        self._render_list2_to_table(self.tableWidget_Sell, with_watch_button=True)

    def _render_list2_to_table(self, table: Optional[QTableWidget], with_watch_button: bool) -> None:
        if table is None:
            return
        self._prepare_table_render(table, len(self.list2))
        for row_idx, row in enumerate(self.list2):
            for col_idx, col_name in enumerate(LIST2_COLUMNS):
                self._set_item(table, row_idx, col_idx, self._format_cell(col_name, row.get(col_name), LIST2_MONEY_COLUMNS))
            if with_watch_button:
                watch = row.get("감시 상태", row.get("감시상태", "감시 중"))
                watch_btn = QPushButton("감시 중지" if watch == "감시 중" else "감시 시작")
                watch_btn.clicked.connect(lambda _checked=False, idx=row_idx: self.toggle_sell_watch(idx))
                table.setCellWidget(row_idx, len(LIST2_COLUMNS), watch_btn)
        table.resizeColumnsToContents()

    def _render_list3(self) -> None:
        table = self.tableWidget_Summary
        if table is None:
            return
        self._prepare_table_render(table, len(self.list3))
        for row_idx, row in enumerate(self.list3):
            for col_idx, col_name in enumerate(LIST3_COLUMNS):
                self._set_item(table, row_idx, col_idx, self._format_cell(col_name, row.get(col_name), LIST3_MONEY_COLUMNS))
            delete_btn = QPushButton("삭제")
            delete_btn.clicked.connect(lambda _checked=False, idx=row_idx: self.delete_summary_row(idx))
            table.setCellWidget(row_idx, len(LIST3_COLUMNS), delete_btn)
        table.resizeColumnsToContents()

    def delete_book_row(self, row_idx: int) -> None:
        if not (0 <= row_idx < len(self.list0)):
            return
        self._delete_book_row(self.list0.pop(row_idx))

    def delete_book_row_by_code(self, code: Optional[str]) -> None:
        if not code:
            return
        for row_idx, row in enumerate(self.list0):
            if row.get("종목코드") == code:
                self._delete_book_row(self.list0.pop(row_idx))
                return

    def _delete_book_row(self, row: Dict[str, Any]) -> None:
        code = row.get("종목코드")

        self.list1[:] = [
            item
            for item in self.list1
            if item.get("종목코드") != code
        ]
        self.list2[:] = [
            item
            for item in self.list2
            if item.get("종목코드") != code
        ]
        self.mark_dirty()
        self.refresh_all_tables()

    def delete_summary_row(self, row_idx: int) -> None:
        if not (0 <= row_idx < len(self.list3)):
            return
        self.list3.pop(row_idx)
        self.mark_dirty()
        self._render_list3()

    def toggle_buy_watch(self, row_idx: int) -> None:
        if not (0 <= row_idx < len(self.list1)):
            return
        current = self.list1[row_idx].get("감시상태", "감시 중")
        if current == "감시 중":
            self.order_manager.stop_buy_watch(row_idx)
        else:
            self.order_manager.start_buy_watch(row_idx)

    def toggle_sell_watch(self, row_idx: int) -> None:
        if not (0 <= row_idx < len(self.list2)):
            return
        current = self.list2[row_idx].get("감시 상태", self.list2[row_idx].get("감시상태", "감시 중"))
        self.list2[row_idx]["감시 상태"] = "감시 중지" if current == "감시 중" else "감시 중"
        self.list2[row_idx].pop("감시상태", None)
        self.mark_dirty()
        self._render_list2()

    def save_state(self) -> None:
        rank = self.lineEdit_rank.text().strip() if self.lineEdit_rank else ""
        if self.textEdit_reg:
            self.textEdit_reg.setPlainText(f"우선 순위: {rank}")
        try:
            save_state_file(self.state_path, self.list0, self.list1, self.list2, self.list3, rank)
            self._dirty = False
            QMessageBox.information(self, "저장", "저장이 완료되었습니다.")
        except Exception as exc:
            QMessageBox.critical(self, "저장 실패", f"저장 중 오류가 발생했습니다.\n{exc}")

    def load_state(self) -> None:
        if not self.state_path.exists():
            return
        try:
            state = load_state_file(self.state_path)
            self.list0[:] = state["list0"]
            self.list1[:] = state["list1"]
            self.list2[:] = state["list2"]
            self.list3[:] = state["list3"]
            rank = state["rank"]
            if self.lineEdit_rank:
                self.lineEdit_rank.setText(rank)
            if self.textEdit_reg and rank:
                self.textEdit_reg.setPlainText(f"우선 순위: {rank}")
            self._dirty = False
        except Exception as exc:
            QMessageBox.warning(self, "불러오기 실패", f"저장 파일을 읽는 중 오류가 발생했습니다.\n{exc}")

    def closeEvent(self, event) -> None:  # type: ignore[override]
        if not self._dirty:
            event.accept()
            return

        result = QMessageBox.question(
            self,
            "저장 확인",
            "저장하시겠습니까?",
            QMessageBox.Yes | QMessageBox.No | QMessageBox.Cancel,
            QMessageBox.Yes,
        )
        if result == QMessageBox.Yes:
            self.save_state()
            if self._dirty:
                event.ignore()
                return
            event.accept()
        elif result == QMessageBox.No:
            event.accept()
        else:
            event.ignore()


def main() -> None:
    app = QApplication(sys.argv)
    window = DMainWindow()
    window.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
