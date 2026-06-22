import csv
import json
import os
import re
import sys
import threading
import time as time_module
import traceback
import urllib.request
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from PyQt5 import uic
from PyQt5.QtCore import Qt, QTimer
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
    from PyQt5.QAxContainer import QAxWidget
except Exception:
    QAxWidget = None

try:
    from pykrx import stock
except Exception:
    stock = None

from watch_selection import DEMO_OPEN_PRICES, select_watch_candidates
from order_manager import (
    BUY_STATUS_ACCEPTED,
    BUY_STATUS_CANCEL_REQUESTED,
    BUY_STATUS_FILLED,
    BUY_STATUS_PARTIAL_WAIT,
    BUY_STATUS_REQUESTED,
    AccountBalanceSyncEvent,
    BuyCancelConfirmedEvent,
    BuyFillEvent,
    BuyOrderAcceptedEvent,
    BuyOrderFailedEvent,
    BuySellOrderManager,
    MarketSellAcceptedEvent,
    RealtimeQuote,
    SellCancelConfirmedEvent,
    SellFillEvent,
    TradeFillSyncEvent,
    to_int,
)
from trade_summary import append_trade_summaries_csv, save_trade_logs_txt
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


@dataclass
class TestQuote:
    code: str
    open_price: int
    low_price: int
    current_price: int
    accumulated_volume: int
    event_time: datetime


@dataclass
class QueuedKiwoomOrder:
    priority: int
    sequence: int
    rqname: str
    order_type: int
    code: str
    qty: int
    price: int
    hoga: str
    original_order_no: str
    on_sent: Optional[Callable[[bool], None]] = None


class SlackWebhookNotifier:
    ENV_NAME = "SLACK_WEBHOOK_URL"

    def send(self, message: str) -> None:
        webhook_url = os.environ.get(self.ENV_NAME, "").strip()
        if not webhook_url:
            return
        threading.Thread(
            target=self._post,
            args=(webhook_url, message),
            daemon=True,
        ).start()

    @staticmethod
    def _post(webhook_url: str, message: str) -> None:
        payload = json.dumps({"text": message}, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                response.read()
        except Exception as exc:
            print(f"[Slack] 체결 알림 전송 실패: {exc}", file=sys.stderr)


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
        self.mock_orders = False

    def send_buy_order(self, row: Dict[str, Any]) -> bool:
        name = row.get("종목명") or row.get("종목코드") or ""
        if self.mock_orders or row.get("_모의테스트"):
            self.buy_log(f"{name}: [모의] 매수 주문")
            return True
        self.buy_log(f"{name}: 키움 API 미연결 - 매수 주문을 실행하지 않았습니다.")
        return False

    def cancel_buy_order(self, row: Dict[str, Any]) -> bool:
        name = row.get("종목명") or row.get("종목코드") or ""
        if self.mock_orders or row.get("_모의테스트"):
            self.buy_log(f"{name}: [모의] 매수 취소")
            return True
        self.buy_log(f"{name}: 키움 API 미연결 - 매수 취소를 실행하지 않았습니다.")
        return False

    def send_sell_order(self, row: Dict[str, Any], price: int, qty: int) -> Optional[str]:
        name = row.get("종목명") or row.get("종목코드") or ""
        if self.mock_orders or row.get("_모의테스트"):
            order_no = f"TEST_SELL{to_int(row.get('매도 횟수')) + qty}_{price}"
            self.sell_log(f"{name}: [모의] 매도 주문 ({price:,}원/{qty}주)")
            return order_no
        self.sell_log(f"{name}: 키움 API 미연결 - 매도 주문을 실행하지 않았습니다. ({price:,}원/{qty}주)")
        return None

    def cancel_sell_orders(self, row: Dict[str, Any]) -> bool:
        name = row.get("종목명") or row.get("종목코드") or ""
        if self.mock_orders or row.get("_모의테스트"):
            self.sell_log(f"{name}: [모의] 잔여 지정가 매도 취소")
            return True
        self.sell_log(f"{name}: 키움 API 미연결 - 잔여 지정가 매도 취소를 실행하지 않았습니다.")
        return False

    def cancel_sell_order(self, row: Dict[str, Any], order_no: str) -> bool:
        name = row.get("종목명") or row.get("종목코드") or ""
        if self.mock_orders or row.get("_모의테스트"):
            self.sell_log(f"{name}: [모의] 지정가 매도 취소 ({order_no})")
            return True
        self.sell_log(f"{name}: 키움 API 미연결 - 지정가 매도 취소를 실행하지 않았습니다. ({order_no})")
        return False

    def cancel_sell_order_qty(self, row: Dict[str, Any], order_no: str, qty: int) -> bool:
        name = row.get("종목명") or row.get("종목코드") or ""
        if self.mock_orders or row.get("_모의테스트"):
            self.sell_log(f"{name}: [모의] 지정가 매도 부분 취소 ({order_no}/{qty}주)")
            return True
        self.sell_log(f"{name}: 키움 API 미연결 - 지정가 매도 부분 취소를 실행하지 않았습니다. ({order_no}/{qty}주)")
        return False

    def send_market_sell_order(self, row: Dict[str, Any], qty: int) -> Optional[str]:
        name = row.get("종목명") or row.get("종목코드") or ""
        if self.mock_orders or row.get("_모의테스트"):
            row["_모의시장가수량"] = to_int(row.get("_모의시장가수량")) + qty
            self.sell_log(f"{name}: [모의] 시장가 매도 ({qty}주)")
            return "TEST_MARKET"
        self.sell_log(f"{name}: 키움 API 미연결 - 시장가 매도를 실행하지 않았습니다. ({qty}주)")
        return None

    def start_realtime_watch(self, row: Dict[str, Any]) -> None:
        name = row.get("종목명") or row.get("종목코드") or ""
        self.buy_log(f"{name}: 키움 API 미연결 - 실시간 감시를 시작하지 않았습니다.")

    def stop_realtime_watch(self, row: Dict[str, Any]) -> None:
        name = row.get("종목명") or row.get("종목코드") or ""
        self.buy_log(f"{name}: 키움 API 미연결 - 실시간 감시를 중단하지 않았습니다.")

    def request_account_balance_sync(self, reason: str = "") -> bool:
        return False

    def request_trade_fill_sync(self, row: Dict[str, Any], reason: str = "") -> bool:
        return False


class KiwoomRealtimeClient(PendingKiwoomOrderClient):
    ORDER_SEND_INTERVAL_SECONDS = 0.7
    ORDER_PRIORITY_CANCEL = 10
    ORDER_PRIORITY_STOP_MARKET = 20
    ORDER_PRIORITY_BUY = 40
    ORDER_PRIORITY_SELL1 = 51
    ORDER_PRIORITY_SELL2 = 52
    ORDER_PRIORITY_SELL3 = 53

    def __init__(self, buy_log, sell_log, quote_handler) -> None:
        super().__init__(buy_log, sell_log)
        self.quote_handler = quote_handler
        self.ocx = None
        self.account_no = ""
        self.logged_in = False
        self.list0_rows_getter = None
        self.realtime_rows_getter = None
        self.pending_code_by_rqname: Dict[str, str] = {}
        self.poll_timer: Optional[QTimer] = None
        self.real_screen_no = "9200"
        self.real_registered_codes: Set[str] = set()
        self._login_started = False
        self.order_manager: Optional[BuySellOrderManager] = None
        self._screen_no = 9100
        self._pending_buy_by_code: Dict[str, Dict[str, Any]] = {}
        self._pending_sell_by_code: Dict[str, List[Dict[str, Any]]] = {}
        self._pending_market_sell_by_code: Dict[str, List[Dict[str, Any]]] = {}
        self._order_meta_by_no: Dict[str, Dict[str, Any]] = {}
        self._filled_qty_by_order_no: Dict[str, int] = {}
        self._last_balance_sync_requested_at = 0.0
        self._balance_sync_pending = False
        self._pending_trade_fill_sync_by_rqname: Dict[str, str] = {}
        self._last_order_sent_at = 0.0
        self._order_queue: List[QueuedKiwoomOrder] = []
        self._order_queue_sequence = 0
        self._order_queue_timer = QTimer()
        self._order_queue_timer.setSingleShot(True)
        self._order_queue_timer.timeout.connect(self._drain_order_queue)
        self._list0_poll_index = 0
        self.slack_notifier = SlackWebhookNotifier()

    def is_available(self) -> bool:
        return QAxWidget is not None

    def login(self) -> bool:
        if self.logged_in:
            self.buy_log(f"[키움] 이미 로그인되어 있습니다. 계좌: {self.account_no}")
            return True
        if QAxWidget is None:
            self.buy_log("[키움] PyQt5.QAxContainer를 사용할 수 없습니다. 32비트 키움 환경에서 실행해 주세요.")
            return False
        if self.ocx is None:
            self.ocx = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")
            if self.ocx.isNull():
                self.buy_log("[키움] OpenAPI+ OCX를 생성하지 못했습니다.")
                self.ocx = None
                return False
            self.ocx.OnEventConnect.connect(self._on_event_connect)
            self.ocx.OnReceiveTrData.connect(self._on_receive_tr_data)
            self.ocx.OnReceiveChejanData.connect(self._on_receive_chejan_data)
            self.ocx.OnReceiveRealData.connect(self._on_receive_real_data)

        result = self.ocx.dynamicCall("CommConnect()")
        if result != 0:
            self.buy_log(f"[키움] 로그인 창 호출 실패: {result}")
            return False
        self._login_started = True
        self.buy_log("[키움] 로그인 창을 열었습니다. 로그인 완료 후 감시를 시작합니다.")
        return True

    def start_quote_watch(self, list0_rows_getter, realtime_rows_getter) -> None:
        if not self.logged_in:
            self.buy_log("[키움] 로그인 전이라 실시간 감시를 시작할 수 없습니다.")
            return
        self.list0_rows_getter = list0_rows_getter
        self.realtime_rows_getter = realtime_rows_getter
        if self.poll_timer is None:
            self.poll_timer = QTimer()
            self.poll_timer.timeout.connect(self._poll_list0_quotes)
        if not self.poll_timer.isActive():
            self.poll_timer.start(1000)
        self._update_real_registrations()
        self.buy_log("[키움] list0 TR 1초 조회, list1/list2 실시간 감시를 시작했습니다.")
        self.sell_log("[키움] list0 TR 1초 조회, list1/list2 실시간 감시를 시작했습니다.")
        self._poll_list0_quotes()

    def stop_quote_watch(self) -> None:
        if self.poll_timer and self.poll_timer.isActive():
            self.poll_timer.stop()
        self._remove_all_real_registrations()
        self.buy_log("[키움] 가격 감시를 중지했습니다.")
        self.sell_log("[키움] 가격 감시를 중지했습니다.")

    def start_realtime_watch(self, row: Dict[str, Any]) -> None:
        name = row.get("종목명") or row.get("종목코드") or ""
        self.buy_log(f"{name}: 실시간 가격 감시 대상입니다.")
        self._update_real_registrations()

    def stop_realtime_watch(self, row: Dict[str, Any]) -> None:
        name = row.get("종목명") or row.get("종목코드") or ""
        self.buy_log(f"{name}: 실시간 가격 감시 대상에서 제외됩니다.")
        self._update_real_registrations()

    def request_account_balance_sync(self, reason: str = "") -> bool:
        if self.mock_orders or self.ocx is None or not self.logged_in or not self.account_no:
            return False
        now = time_module.monotonic()
        if self._balance_sync_pending and now - self._last_balance_sync_requested_at < 2.0:
            return False
        if now - self._last_balance_sync_requested_at < 1.0:
            return False

        rqname = "ACCOUNT_BALANCE_SYNC"
        self._last_balance_sync_requested_at = now
        self._balance_sync_pending = True
        self.ocx.dynamicCall("SetInputValue(QString, QString)", "계좌번호", self.account_no)
        self.ocx.dynamicCall("SetInputValue(QString, QString)", "비밀번호", "")
        self.ocx.dynamicCall("SetInputValue(QString, QString)", "비밀번호입력매체구분", "00")
        self.ocx.dynamicCall("SetInputValue(QString, QString)", "조회구분", "2")
        result = self.ocx.dynamicCall(
            "CommRqData(QString, QString, int, QString)",
            rqname,
            "opw00018",
            0,
            str(self._next_screen_no()),
        )
        if result != 0:
            self._balance_sync_pending = False
            self.sell_log(f"[키움] HTS 잔고 동기화 요청 실패: {result}")
            return False
        if reason:
            self.sell_log(f"[키움] HTS 잔고 동기화 요청: {reason}")
        return True

    def request_trade_fill_sync(self, row: Dict[str, Any], reason: str = "") -> bool:
        if self.mock_orders or self.ocx is None or not self.logged_in or not self.account_no:
            return False
        code = str(row.get("종목코드", ""))
        if not code:
            return False

        rqname = f"TRADE_FILL_SYNC_{code}_{datetime.now().strftime('%H%M%S%f')}"
        self._pending_trade_fill_sync_by_rqname[rqname] = code
        self.ocx.dynamicCall("SetInputValue(QString, QString)", "계좌번호", self.account_no)
        self.ocx.dynamicCall("SetInputValue(QString, QString)", "비밀번호", "")
        self.ocx.dynamicCall("SetInputValue(QString, QString)", "비밀번호입력매체구분", "00")
        self.ocx.dynamicCall("SetInputValue(QString, QString)", "종목코드", code)
        result = self.ocx.dynamicCall(
            "CommRqData(QString, QString, int, QString)",
            rqname,
            "opt10072",
            0,
            str(self._next_screen_no()),
        )
        if result != 0:
            self._pending_trade_fill_sync_by_rqname.pop(rqname, None)
            self.sell_log(f"[키움] HTS 평균 매도가 보정 요청 실패 {code}: {result}")
            return False
        name = row.get("종목명") or code
        self.sell_log(f"[키움] HTS 평균 매도가 보정 요청: {name} ({reason})")
        return True

    def send_buy_order(self, row: Dict[str, Any]) -> bool:
        if self.mock_orders or row.get("_모의테스트"):
            return super().send_buy_order(row)
        code = str(row.get("종목코드", ""))
        qty = to_int(row.get("매수 수량"))
        price = to_int(row.get("매수가"))
        if qty <= 0 or price <= 0:
            self.buy_log(f"{self._name(row)}: 매수 주문 수량/가격 오류")
            return False
        def on_sent(ok: bool) -> None:
            if not ok:
                if self.order_manager is not None:
                    self.order_manager.handle_buy_order_failed(
                        BuyOrderFailedEvent(code=code, order_no="", reason="매수 주문 전송 실패")
                    )
                return
            self._pending_buy_by_code[code] = {"row": row, "qty": qty, "price": price}
            self.buy_log(f"{self._name(row)}: 키움 매수 주문 요청 {price:,}원/{qty}주")

        ok = self._queue_order("BUY", 1, code, qty, price, "00", "", self.ORDER_PRIORITY_BUY, on_sent)
        return ok

    def cancel_buy_order(self, row: Dict[str, Any]) -> bool:
        if self.mock_orders or row.get("_모의테스트"):
            return super().cancel_buy_order(row)
        code = str(row.get("종목코드", ""))
        order_no = str(row.get("매수주문번호", ""))
        qty = to_int(row.get("매수미체결수량")) or to_int(row.get("매수 수량"))
        def on_sent(ok: bool) -> None:
            if not ok:
                return
            self.buy_log(f"{self._name(row)}: 키움 매수 취소 요청 {qty}주")

        ok = self._queue_order("BUY_CANCEL", 3, code, qty, 0, "00", order_no, self.ORDER_PRIORITY_CANCEL, on_sent)
        return ok

    def send_sell_order(self, row: Dict[str, Any], price: int, qty: int) -> Optional[str]:
        if self.mock_orders or row.get("_모의테스트"):
            return super().send_sell_order(row, price, qty)
        code = str(row.get("종목코드", ""))
        is_stop_limit = bool(row.pop("손절지정가요청중", False))
        level = self._infer_sell_level(row, price, qty)
        pending_order_no = f"PENDING_{code}_{self._order_queue_sequence + 1}"
        def on_sent(ok: bool) -> None:
            if not ok:
                return
            meta_level = 0 if is_stop_limit else level
            self._pending_sell_by_code.setdefault(code, []).append(
                {
                    "row": row,
                    "level": meta_level,
                    "qty": qty,
                    "price": price,
                    "market": False,
                    "stop_limit": is_stop_limit,
                }
            )
            if is_stop_limit:
                self.sell_log(f"{self._name(row)}: 키움 손절 지정가 주문 요청 {price:,}원/{qty}주")
            else:
                self.sell_log(f"{self._name(row)}: 키움 매도 주문 요청 매도가{level} {price:,}원/{qty}주")

        priority = self.ORDER_PRIORITY_STOP_MARKET if is_stop_limit else self.ORDER_PRIORITY_SELL1 + max(0, min(level, 3) - 1)
        rqname = "STOP_LIMIT_SELL" if is_stop_limit else f"SELL{level}"
        queued = self._queue_order(rqname, 2, code, qty, price, "00", "", priority, on_sent)
        return pending_order_no if queued else None

    def cancel_sell_orders(self, row: Dict[str, Any]) -> bool:
        if self.mock_orders or row.get("_모의테스트"):
            return super().cancel_sell_orders(row)
        code = str(row.get("종목코드", ""))
        removed_queued = self._remove_queued_limit_sell_orders(code)
        ok_any = False
        for level in (1, 2, 3):
            order_no = str(row.get(f"매도가{level}주문번호", ""))
            qty = self._remaining_sell_qty(row, level)
            if order_no and not self._is_pending_order_no(order_no) and qty > 0:
                ok_any = self.cancel_sell_order_qty(row, order_no, qty) or ok_any
        return ok_any or removed_queued > 0

    def cancel_sell_order(self, row: Dict[str, Any], order_no: str) -> bool:
        if self.mock_orders or row.get("_모의테스트"):
            return super().cancel_sell_order(row, order_no)
        level = self._level_by_order_no(row, order_no)
        qty = self._remaining_sell_qty(row, level) if level else to_int(row.get("잔여 수량"))
        return self.cancel_sell_order_qty(row, order_no, qty)

    def cancel_sell_order_qty(self, row: Dict[str, Any], order_no: str, qty: int) -> bool:
        if self.mock_orders or row.get("_모의테스트"):
            return super().cancel_sell_order_qty(row, order_no, qty)
        code = str(row.get("종목코드", ""))
        def on_sent(ok: bool) -> None:
            if not ok:
                return
            self.sell_log(f"{self._name(row)}: 키움 매도 취소 요청 {order_no}/{qty}주")

        ok = self._queue_order(
            "SELL_CANCEL",
            4,
            code,
            qty,
            0,
            "00",
            order_no,
            self.ORDER_PRIORITY_CANCEL,
            on_sent,
        )
        return ok

    def send_market_sell_order(self, row: Dict[str, Any], qty: int) -> Optional[str]:
        if self.mock_orders or row.get("_모의테스트"):
            return super().send_market_sell_order(row, qty)
        code = str(row.get("종목코드", ""))
        self._remove_queued_limit_sell_orders(code)

        def on_sent(ok: bool) -> None:
            if not ok:
                return
            self._pending_market_sell_by_code.setdefault(code, []).append(
                {"row": row, "level": 0, "qty": qty, "price": 0, "market": True}
            )
            self.sell_log(f"{self._name(row)}: 키움 시장가 매도 요청 {qty}주")

        self._queue_order("MARKET_SELL", 2, code, qty, 0, "03", "", self.ORDER_PRIORITY_STOP_MARKET, on_sent)
        return None

    def _on_event_connect(self, err_code: int) -> None:
        self._login_started = False
        if err_code != 0:
            self.logged_in = False
            self.buy_log(f"[키움] 로그인 실패: {err_code}")
            return
        accounts = str(self.ocx.dynamicCall("GetLoginInfo(QString)", "ACCNO") or "")
        account_list = [account.strip() for account in accounts.split(";") if account.strip()]
        self.account_no = account_list[0] if account_list else ""
        self.logged_in = True
        user_id = self.ocx.dynamicCall("GetLoginInfo(QString)", "USER_ID")
        self.buy_log(f"[키움] 로그인 성공: {user_id}, 계좌: {self.account_no}")
        self.quote_handler()

    def _list0_codes(self) -> List[str]:
        rows = self.list0_rows_getter() if self.list0_rows_getter else []
        codes = []
        for row in rows:
            code = str(row.get("종목코드", "")).strip()
            if code and code not in codes:
                codes.append(code)
        return codes

    def _realtime_codes(self) -> Set[str]:
        rows = self.realtime_rows_getter() if self.realtime_rows_getter else []
        codes: Set[str] = set()
        for row in rows:
            code = str(row.get("종목코드", "")).strip()
            if code:
                codes.add(code)
        return codes

    def _poll_list0_quotes(self) -> None:
        if not self.logged_in or self.ocx is None:
            return
        self._update_real_registrations()
        codes = self._list0_codes()
        if not codes:
            self._list0_poll_index = 0
            return
        if self._list0_poll_index >= len(codes):
            self._list0_poll_index = 0
        code = codes[self._list0_poll_index]
        self._list0_poll_index = (self._list0_poll_index + 1) % len(codes)
        rqname = f"LIST0_{code}_{datetime.now().strftime('%H%M%S%f')}"
        self.pending_code_by_rqname[rqname] = code
        self.ocx.dynamicCall("SetInputValue(QString, QString)", "종목코드", code)
        result = self.ocx.dynamicCall(
            "CommRqData(QString, QString, int, QString)",
            rqname,
            "opt10001",
            0,
            "9001",
        )
        if result != 0:
            self.pending_code_by_rqname.pop(rqname, None)
            self.buy_log(f"[키움] list0 현재가 요청 실패: {code} ({result})")

    def _update_real_registrations(self) -> None:
        if not self.logged_in or self.ocx is None:
            return
        next_codes = self._realtime_codes()
        if next_codes == self.real_registered_codes:
            return
        for code in sorted(self.real_registered_codes):
            self.ocx.dynamicCall("SetRealRemove(QString, QString)", self.real_screen_no, code)
        self.real_registered_codes = set()
        if not next_codes:
            return
        code_text = ";".join(sorted(next_codes))
        # 10 현재가, 16 시가, 18 저가, 13 누적거래량
        result = self.ocx.dynamicCall(
            "SetRealReg(QString, QString, QString, QString)",
            self.real_screen_no,
            code_text,
            "10;16;18;13",
            "0",
        )
        if result == 0:
            self.real_registered_codes = set(next_codes)
        else:
            self.buy_log(f"[키움] 실시간 등록 실패: {code_text} ({result})")

    def _remove_all_real_registrations(self) -> None:
        if self.ocx is None:
            self.real_registered_codes.clear()
            return
        for code in sorted(self.real_registered_codes):
            self.ocx.dynamicCall("SetRealRemove(QString, QString)", self.real_screen_no, code)
        self.real_registered_codes.clear()

    def _on_receive_real_data(self, code, real_type, real_data) -> None:
        if not code:
            return
        current_price = abs(parse_int(self._real_data(code, 10)))
        open_price = abs(parse_int(self._real_data(code, 16)))
        low_price = abs(parse_int(self._real_data(code, 18)))
        volume = abs(parse_int(self._real_data(code, 13)))
        if current_price <= 0:
            return
        if open_price <= 0:
            open_price = current_price
        if low_price <= 0:
            low_price = current_price
        self.quote_handler(
            RealtimeQuote(
                code=str(code).strip(),
                current_price=current_price,
                low_price=low_price,
                accumulated_volume=volume,
                event_time=datetime.now(),
                open_price=open_price,
            )
        )

    def _on_receive_tr_data(self, screen_no, rqname, trcode, record_name, prev_next, *_args) -> None:
        code = self._pending_trade_fill_sync_by_rqname.pop(str(rqname), "")
        if code and str(trcode).lower() == "opt10072":
            sell_qty, average_sell_price = self._parse_daily_realized_sell(trcode, rqname, code)
            if self.order_manager is not None:
                self.order_manager.handle_trade_fill_sync(
                    TradeFillSyncEvent(
                        code=code,
                        buy_qty=0,
                        buy_amount=0,
                        sell_qty=sell_qty,
                        sell_amount=0,
                        average_sell_price=average_sell_price,
                        event_time=datetime.now(),
                    )
                )
            return

        if str(rqname) == "ACCOUNT_BALANCE_SYNC" and str(trcode).lower() == "opw00018":
            self._balance_sync_pending = False
            if str(prev_next) == "2":
                self.sell_log("[키움] HTS 잔고 동기화 응답이 여러 페이지입니다. 안전을 위해 이번 동기화는 건너뜁니다.")
                return
            positions = self._parse_account_balance_positions(trcode, rqname)
            if positions is None:
                return
            if self.order_manager is not None:
                self.order_manager.handle_account_balance_sync(
                    AccountBalanceSyncEvent(positions=positions, event_time=datetime.now())
                )
            return

        code = self.pending_code_by_rqname.pop(str(rqname), "")
        if not code or str(trcode).lower() != "opt10001":
            return
        current_price = abs(parse_int(self._comm_data(trcode, rqname, "현재가")))
        open_price = abs(parse_int(self._comm_data(trcode, rqname, "시가")))
        low_price = abs(parse_int(self._comm_data(trcode, rqname, "저가")))
        volume = abs(parse_int(self._comm_data(trcode, rqname, "거래량")))
        if current_price <= 0:
            return
        if open_price <= 0:
            open_price = current_price
        if low_price <= 0:
            low_price = current_price
        self.quote_handler(
            RealtimeQuote(
                code=code,
                current_price=current_price,
                low_price=low_price,
                accumulated_volume=volume,
                event_time=datetime.now(),
                open_price=open_price,
            )
        )

    def _comm_data(self, trcode, rqname, item_name: str, index: int = 0) -> str:
        value = self.ocx.dynamicCall(
            "GetCommData(QString, QString, int, QString)",
            trcode,
            rqname,
            index,
            item_name,
        )
        return str(value).strip()

    def _parse_account_balance_positions(self, trcode, rqname) -> Optional[Dict[str, int]]:
        positions: Dict[str, int] = {}
        try:
            repeat_count = int(self.ocx.dynamicCall("GetRepeatCnt(QString, QString)", trcode, rqname) or 0)
        except Exception:
            repeat_count = 0

        for idx in range(repeat_count):
            raw_code = (
                self._comm_data(trcode, rqname, "종목번호", idx)
                or self._comm_data(trcode, rqname, "종목코드", idx)
            )
            code = self._normalize_code(raw_code)
            if not code:
                continue
            qty = abs(parse_int(
                self._comm_data(trcode, rqname, "보유수량", idx)
                or self._comm_data(trcode, rqname, "보유량", idx)
            ))
            positions[code] = qty
        if repeat_count > 0 and not positions:
            self.sell_log("[키움] HTS 잔고 동기화 파싱 실패: 보유종목 응답을 읽지 못했습니다.")
            return None
        self.sell_log(f"[키움] HTS 잔고 동기화 완료: {len(positions)}종목")
        return positions

    def _parse_trade_fill_totals(self, trcode, rqname, target_code: str) -> tuple[int, int, int, int]:
        buy_qty = 0
        buy_amount = 0
        sell_qty = 0
        sell_amount = 0
        try:
            repeat_count = int(self.ocx.dynamicCall("GetRepeatCnt(QString, QString)", trcode, rqname) or 0)
        except Exception:
            repeat_count = 0

        for idx in range(repeat_count):
            raw_code = self._first_comm_data(
                trcode,
                rqname,
                idx,
                ["종목코드", "종목번호", "주식코드"],
            )
            code = self._normalize_code(raw_code)
            if code and code != target_code:
                continue

            side_text = self._first_comm_data(
                trcode,
                rqname,
                idx,
                ["주문구분", "매도수구분", "매매구분", "구분"],
            )
            side_key = side_text.strip()
            is_sell = "매도" in side_text or side_key in {"1", "-1"}
            is_buy = "매수" in side_text or side_key in {"2", "+1"}
            if side_text and not is_sell and not is_buy:
                continue

            qty = abs(parse_int(self._first_comm_data(
                trcode,
                rqname,
                idx,
                ["체결량", "체결수량", "단위체결량", "체결수량누계"],
            )))
            if qty <= 0:
                continue

            amount = abs(parse_int(self._first_comm_data(
                trcode,
                rqname,
                idx,
                ["체결금액", "체결금액누계", "거래금액"],
            )))
            if amount <= 0:
                price = abs(parse_int(self._first_comm_data(
                    trcode,
                    rqname,
                    idx,
                    ["체결가", "체결가격", "단위체결가"],
                )))
                amount = price * qty

            if is_buy:
                buy_qty += qty
                buy_amount += amount
            else:
                sell_qty += qty
                sell_amount += amount

        self.sell_log(
            f"[키움] HTS 체결내역 보정 완료 {target_code}: "
            f"매수 {buy_qty}주/{buy_amount:,}원, 매도 {sell_qty}주/{sell_amount:,}원"
        )
        return buy_qty, buy_amount, sell_qty, sell_amount

    def _parse_daily_realized_sell(self, trcode, rqname, target_code: str) -> tuple[int, int]:
        sell_qty = abs(parse_int(self._first_comm_data(
            trcode,
            rqname,
            0,
            ["체결량", "매도수량", "체결수량"],
        )))
        average_sell_price = abs(parse_int(self._first_comm_data(
            trcode,
            rqname,
            0,
            ["체결가", "평균매도가", "매도평균가"],
        )))
        self.sell_log(
            f"[키움] HTS 평균 매도가 보정 완료 {target_code}: "
            f"매도 {sell_qty}주/{average_sell_price:,}원"
        )
        return sell_qty, average_sell_price

    def _first_comm_data(self, trcode, rqname, index: int, item_names: List[str]) -> str:
        for item_name in item_names:
            value = self._comm_data(trcode, rqname, item_name, index)
            if value:
                return value
        return ""

    def _real_data(self, code, fid: int) -> str:
        value = self.ocx.dynamicCall("GetCommRealData(QString, int)", code, fid)
        return str(value).strip()

    def _queue_order(
        self,
        rqname: str,
        order_type: int,
        code: str,
        qty: int,
        price: int,
        hoga: str,
        original_order_no: str,
        priority: int,
        on_sent: Optional[Callable[[bool], None]] = None,
    ) -> bool:
        if not self.logged_in or self.ocx is None or not self.account_no:
            self.buy_log("[키움] 로그인/계좌 확인 전이라 주문을 보낼 수 없습니다.")
            return False
        if qty <= 0:
            log = self.sell_log if order_type in {2, 4} else self.buy_log
            log(f"[키움] 주문 수량 오류 {rqname}/{code}: {qty}")
            return False
        self._order_queue_sequence += 1
        self._order_queue.append(
            QueuedKiwoomOrder(
                priority=priority,
                sequence=self._order_queue_sequence,
                rqname=rqname,
                order_type=order_type,
                code=code,
                qty=qty,
                price=price,
                hoga=hoga,
                original_order_no=original_order_no,
                on_sent=on_sent,
            )
        )
        self._schedule_order_queue()
        return True

    def _schedule_order_queue(self) -> None:
        if self._order_queue_timer.isActive() or not self._order_queue:
            return
        elapsed = time_module.monotonic() - self._last_order_sent_at
        wait_ms = max(0, int((self.ORDER_SEND_INTERVAL_SECONDS - elapsed) * 1000))
        self._order_queue_timer.start(wait_ms)

    def _drain_order_queue(self) -> None:
        if not self._order_queue:
            return
        next_index = min(
            range(len(self._order_queue)),
            key=lambda idx: (self._order_queue[idx].priority, self._order_queue[idx].sequence),
        )
        order = self._order_queue.pop(next_index)
        ok = self._send_order_now(order)
        if order.on_sent is not None:
            order.on_sent(ok)
        if self._order_queue:
            self._schedule_order_queue()

    def _send_order_now(self, order: QueuedKiwoomOrder) -> bool:
        if not self.logged_in or self.ocx is None or not self.account_no:
            self.buy_log("[키움] 로그인/계좌 확인 전이라 주문을 보낼 수 없습니다.")
            return False
        screen_no = str(self._next_screen_no())
        args = [
            order.rqname,
            screen_no,
            self.account_no,
            order.order_type,
            order.code,
            order.qty,
            order.price,
            order.hoga,
            order.original_order_no,
        ]
        try:
            result = self.ocx.dynamicCall(
                "SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)",
                args,
            )
        except Exception as exc:
            log = self.sell_log if order.order_type in {2, 4} else self.buy_log
            log(f"[키움] 주문 요청 예외 {order.rqname}/{order.code}: {exc}")
            return False
        self._last_order_sent_at = time_module.monotonic()
        if result != 0:
            log = self.sell_log if order.order_type in {2, 4} else self.buy_log
            log(f"[키움] 주문 요청 실패 {order.rqname}/{order.code}: {result}")
            return False
        return True

    def _remove_queued_limit_sell_orders(self, code: str) -> int:
        before = len(self._order_queue)
        self._order_queue = [
            order
            for order in self._order_queue
            if not (
                order.code == code
                and order.order_type == 2
                and order.hoga == "00"
                and not order.original_order_no
                and (str(order.rqname).startswith("SELL") or str(order.rqname) == "STOP_LIMIT_SELL")
            )
        ]
        return before - len(self._order_queue)

    def _next_screen_no(self) -> int:
        self._screen_no += 1
        if self._screen_no > 9199:
            self._screen_no = 9100
        return self._screen_no

    def _on_receive_chejan_data(self, gubun, item_cnt, fid_list) -> None:
        if str(gubun) != "0" or self.order_manager is None:
            return
        order_no = self._chejan(9203)
        code = self._normalize_code(self._chejan(9001))
        order_status = self._chejan(913)
        order_type_text = self._chejan(905)
        hoga_text = self._chejan(906)
        buy_sell = self._chejan(907)
        order_price = abs(parse_int(self._chejan(901)))
        unfilled_qty = abs(parse_int(self._chejan(902)))
        fill_qty = abs(parse_int(self._chejan(911)))
        fill_price = abs(parse_int(self._chejan(910)))
        if not code or not order_no:
            return

        is_buy = buy_sell == "2" or "매수" in order_type_text
        is_sell = buy_sell == "1" or "매도" in order_type_text
        is_cancel = "취소" in order_type_text
        meta = self._order_meta_by_no.get(order_no)
        if meta is None:
            if is_cancel:
                meta = self._cancel_order_meta(code, is_buy, is_sell, order_no)
            else:
                meta = self._register_order_meta(order_no, code, is_buy, is_sell, order_type_text, hoga_text, order_price)

        if is_buy:
            self._handle_buy_chejan(code, order_no, order_status, order_type_text, unfilled_qty, fill_qty, fill_price, meta)
            return
        if is_sell:
            self._handle_sell_chejan(code, order_no, order_status, order_type_text, unfilled_qty, fill_qty, fill_price, meta)

    def _handle_buy_chejan(
        self,
        code: str,
        order_no: str,
        order_status: str,
        order_type_text: str,
        unfilled_qty: int,
        fill_qty: int,
        fill_price: int,
        meta: Dict[str, Any],
    ) -> None:
        if "취소" in order_type_text:
            if "확인" in order_status or unfilled_qty == 0:
                self.buy_log(
                    f"[키움] 매수 취소 확인 {code}/{order_no}: 상태={order_status}, 구분={order_type_text}, 미체결={unfilled_qty}"
                )
                self.order_manager.handle_buy_cancel_confirmed(
                    BuyCancelConfirmedEvent(code=code, order_no=order_no, unfilled_qty=unfilled_qty)
                )
                self.request_account_balance_sync("매수 취소 확인")
            else:
                self.buy_log(
                    f"[키움] 매수 취소 이벤트 대기 {code}/{order_no}: 상태={order_status}, 구분={order_type_text}, 미체결={unfilled_qty}"
                )
            return

        if "접수" in order_status or "확인" in order_status:
            self.order_manager.handle_buy_order_accepted(BuyOrderAcceptedEvent(code=code, order_no=order_no))
        if fill_qty > 0:
            row = meta.get("row") or self._find_list1_row(code)
            order_qty = to_int(meta.get("qty"))
            cumulative_qty = self._reported_cumulative_fill_qty(order_no, order_qty, unfilled_qty, fill_qty)
            new_fill_qty = cumulative_qty - self._filled_qty_by_order_no.get(order_no, 0)
            if new_fill_qty <= 0:
                return
            self._filled_qty_by_order_no[order_no] = cumulative_qty
            self.order_manager.handle_buy_fill(
                BuyFillEvent(
                    code=code,
                    order_no=order_no,
                    fill_qty=new_fill_qty,
                    cumulative_filled_qty=cumulative_qty,
                    unfilled_qty=unfilled_qty,
                    fill_price=fill_price,
                    event_time=datetime.now(),
                )
            )
            self._notify_slack_buy_fill(
                row,
                code,
                order_no,
                fill_price,
                new_fill_qty,
                cumulative_qty,
                unfilled_qty,
            )
            self.request_account_balance_sync("매수 체결")

    def _handle_sell_chejan(
        self,
        code: str,
        order_no: str,
        order_status: str,
        order_type_text: str,
        unfilled_qty: int,
        fill_qty: int,
        fill_price: int,
        meta: Dict[str, Any],
    ) -> None:
        if meta.get("market") and ("접수" in order_status or "확인" in order_status):
            self.order_manager.handle_market_sell_accepted(MarketSellAcceptedEvent(code=code, order_no=order_no))
        if fill_qty > 0 and "취소" not in order_type_text:
            row = self._find_list2_row(code)
            if row is None:
                return
            sell_level = self._verified_sell_level(row, order_no, meta)
            if sell_level is None:
                self.sell_log(f"[키움] 매도 체결 레벨 확인 실패 {code}/{order_no}: 체결 이벤트 무시")
                return
            order_qty = (
                to_int(row.get(f"매도가{sell_level}주문수량"))
                if sell_level in {1, 2, 3}
                else to_int(meta.get("qty"))
            )
            cumulative_qty = self._reported_cumulative_fill_qty(order_no, order_qty, unfilled_qty, fill_qty)
            new_fill_qty = cumulative_qty - self._filled_qty_by_order_no.get(order_no, 0)
            applied_fill_qty = min(new_fill_qty, max(0, to_int(row.get("잔여 수량"))))
            if applied_fill_qty <= 0:
                return
            self._filled_qty_by_order_no[order_no] = cumulative_qty
            remaining_qty = max(0, to_int(row.get("잔여 수량")) - applied_fill_qty)
            self.order_manager.handle_sell_fill(
                SellFillEvent(
                    code=code,
                    order_no=order_no,
                    sell_level=sell_level,
                    fill_qty=applied_fill_qty,
                    fill_price=fill_price,
                    remaining_holding_qty=remaining_qty,
                    event_time=datetime.now(),
                )
            )
            self._notify_slack_sell_fill(
                row,
                code,
                order_no,
                sell_level,
                fill_price,
                applied_fill_qty,
                remaining_qty,
            )
            self.request_account_balance_sync("매도 체결")
        if "취소" in order_type_text and ("확인" in order_status or unfilled_qty == 0):
            row = meta.get("row") or self._find_list2_row(code)
            remaining_qty = to_int(row.get("잔여 수량")) if row else 0
            self.sell_log(
                f"[키움] 매도 취소 확인 {code}/{order_no}: 상태={order_status}, 구분={order_type_text}, 미체결={unfilled_qty}"
            )
            self.order_manager.handle_sell_cancel_confirmed(
                SellCancelConfirmedEvent(code=code, remaining_holding_qty=remaining_qty)
            )
            self.request_account_balance_sync("매도 취소 확인")
        elif "취소" in order_type_text:
            self.sell_log(
                f"[키움] 매도 취소 이벤트 대기 {code}/{order_no}: 상태={order_status}, 구분={order_type_text}, 미체결={unfilled_qty}"
            )

    def _register_order_meta(
        self,
        order_no: str,
        code: str,
        is_buy: bool,
        is_sell: bool,
        order_type_text: str = "",
        hoga_text: str = "",
        order_price: int = 0,
    ) -> Dict[str, Any]:
        if is_buy:
            meta = self._pending_buy_by_code.pop(code, {"row": self._find_list1_row(code)})
            meta.update({"side": "buy", "level": 0, "market": False})
            self._order_meta_by_no[order_no] = meta
            return meta

        pending_list = (
            self._pending_market_sell_by_code.setdefault(code, [])
            if self._is_market_sell_event(order_no, code, is_sell, order_type_text, hoga_text, order_price)
            else self._pending_sell_by_code.setdefault(code, [])
        )
        meta = self._pop_matching_order_meta(pending_list, order_price)
        if meta is None:
            meta = {"row": self._find_list2_row(code), "level": 0, "market": False}
        row = meta.get("row")
        level = to_int(meta.get("level"))
        if row is not None:
            if meta.get("market"):
                row["시장가주문번호"] = order_no
            elif meta.get("stop_limit"):
                row["손절지정가주문번호"] = order_no
            elif level in {1, 2, 3}:
                row[f"매도가{level}주문번호"] = order_no
                self._sync_row_sell_order_numbers(row)
            if self.order_manager is not None:
                self.order_manager.retry_pending_sell_action(code)
        self._order_meta_by_no[order_no] = meta
        return meta

    def _pop_matching_order_meta(self, pending_list: List[Dict[str, Any]], order_price: int) -> Optional[Dict[str, Any]]:
        if not pending_list:
            return None
        if order_price > 0:
            for idx, meta in enumerate(pending_list):
                if to_int(meta.get("price")) == order_price:
                    return pending_list.pop(idx)
        return pending_list.pop(0)

    def _cancel_order_meta(self, code: str, is_buy: bool, is_sell: bool, order_no: str) -> Dict[str, Any]:
        if is_buy:
            return {"row": self._find_list1_row(code), "side": "buy", "level": 0, "market": False}

        row = self._find_list2_row(code) if is_sell else None
        level = self._level_by_order_no(row, order_no) if row is not None else 0
        return {"row": row, "side": "sell", "level": level, "market": False}

    def _is_market_sell_event(
        self,
        order_no: str,
        code: str,
        is_sell: bool,
        order_type_text: str = "",
        hoga_text: str = "",
        order_price: int = 0,
    ) -> bool:
        if not is_sell:
            return False
        meta = self._order_meta_by_no.get(order_no)
        if meta is not None:
            return bool(meta.get("market"))
        if "시장" in str(hoga_text) or "시장" in str(order_type_text):
            return True
        if order_price == 0 and self._pending_market_sell_by_code.get(code):
            return True
        if self._pending_market_sell_by_code.get(code) and not self._pending_sell_by_code.get(code):
            return True
        return False

    def _chejan(self, fid: int) -> str:
        value = self.ocx.dynamicCall("GetChejanData(int)", fid)
        return str(value).strip()

    def _normalize_code(self, raw: str) -> str:
        code = re.sub(r"[^0-9A-Za-z]", "", raw or "").upper()
        if code.startswith("A") and code[1:].isdigit():
            code = code[1:]
        return code.zfill(6) if code.isdigit() else code

    def _notify_slack_buy_fill(
        self,
        row: Optional[Dict[str, Any]],
        code: str,
        order_no: str,
        fill_price: int,
        fill_qty: int,
        cumulative_qty: int,
        unfilled_qty: int,
    ) -> None:
        name = self._name(row or {"종목코드": code})
        self.slack_notifier.send(
            "\n".join(
                [
                    "[매수 체결]",
                    f"종목: {name} ({code})",
                    f"체결가: {fill_price:,}원",
                    f"체결수량: {fill_qty:,}주",
                    f"누적체결: {cumulative_qty:,}주",
                    f"미체결: {unfilled_qty:,}주",
                    f"주문번호: {order_no}",
                ]
            )
        )

    def _notify_slack_sell_fill(
        self,
        row: Optional[Dict[str, Any]],
        code: str,
        order_no: str,
        sell_level: int,
        fill_price: int,
        fill_qty: int,
        remaining_qty: int,
    ) -> None:
        name = self._name(row or {"종목코드": code})
        sell_type = f"매도가{sell_level}" if sell_level in {1, 2, 3} else "손절/시장가"
        self.slack_notifier.send(
            "\n".join(
                [
                    "[매도 체결]",
                    f"종목: {name} ({code})",
                    f"구분: {sell_type}",
                    f"체결가: {fill_price:,}원",
                    f"체결수량: {fill_qty:,}주",
                    f"잔여수량: {remaining_qty:,}주",
                    f"주문번호: {order_no}",
                ]
            )
        )

    def _name(self, row: Dict[str, Any]) -> str:
        return str(row.get("종목명") or row.get("종목코드") or "")

    def _is_pending_order_no(self, order_no: str) -> bool:
        return str(order_no).startswith("PENDING_")

    def _infer_sell_level(self, row: Dict[str, Any], price: int, qty: int) -> int:
        for level in (1, 2, 3):
            if to_int(row.get(f"매도가{level}")) == to_int(price):
                return level
        for level in (1, 2, 3):
            if to_int(row.get(f"매도가{level}주문수량")) == to_int(qty):
                return level
        return 1

    def _level_by_order_no(self, row: Dict[str, Any], order_no: str) -> int:
        for level in (1, 2, 3):
            if str(row.get(f"매도가{level}주문번호", "")) == str(order_no):
                return level
        return 0

    def _verified_sell_level(self, row: Optional[Dict[str, Any]], order_no: str, meta: Dict[str, Any]) -> Optional[int]:
        if row is None:
            return to_int(meta.get("level"))
        if meta.get("market"):
            return 0
        if meta.get("stop_limit"):
            return 0 if str(row.get("손절지정가주문번호", "")) == str(order_no) else None

        level = self._level_by_order_no(row, order_no)
        if level in {1, 2, 3}:
            return level
        meta_level = to_int(meta.get("level"))
        if meta_level in {1, 2, 3} and self._is_pending_order_no(str(row.get(f"매도가{meta_level}주문번호", ""))):
            return meta_level
        return None

    def _reported_cumulative_fill_qty(
        self,
        order_no: str,
        order_qty: int,
        unfilled_qty: int,
        event_fill_qty: int,
    ) -> int:
        previous_qty = self._filled_qty_by_order_no.get(order_no, 0)
        if order_qty > 0 and 0 <= unfilled_qty <= order_qty:
            return max(previous_qty, order_qty - unfilled_qty)
        return previous_qty + event_fill_qty

    def _remaining_sell_qty(self, row: Dict[str, Any], level: int) -> int:
        if level not in {1, 2, 3}:
            return to_int(row.get("잔여 수량"))
        return max(0, to_int(row.get(f"매도가{level}주문수량")) - to_int(row.get(f"매도가{level}체결수량")))

    def _sync_row_sell_order_numbers(self, row: Dict[str, Any]) -> None:
        row["매도주문번호들"] = ",".join(
            order_no
            for order_no in [
                str(row.get("매도가1주문번호", "")),
                str(row.get("매도가2주문번호", "")),
                str(row.get("매도가3주문번호", "")),
            ]
            if order_no
        )

    def _find_list1_row(self, code: str) -> Optional[Dict[str, Any]]:
        if self.order_manager is None:
            return None
        for row in self.order_manager.list1:
            if str(row.get("종목코드", "")) == str(code):
                return row
        return None

    def _find_list2_row(self, code: str) -> Optional[Dict[str, Any]]:
        if self.order_manager is None:
            return None
        for row in self.order_manager.list2:
            if str(row.get("종목코드", "")) == str(code):
                return row
        return None


class DMainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.base_dir = Path(__file__).resolve().parent
        self.state_path = self.base_dir / "D_Ver1_state.json"
        self.summary_csv_path = self.base_dir / "trade_summary.csv"
        self.replay_csv_path = self.base_dir / "data" / "replay_quotes.csv"
        self._dirty = False
        self._csv_replay_rows: List[Dict[str, str]] = []
        self._csv_replay_index = 0

        ui_path = self._resolve_ui_path()
        uic.loadUi(str(ui_path), self)

        self.list0: List[Dict[str, Any]] = []
        self.list1: List[Dict[str, Any]] = []
        self.list2: List[Dict[str, Any]] = []
        self.list3: List[Dict[str, Any]] = []
        self._priority_selection_done = False
        self._buy_watch_active = False
        self._market_open_watch_time = time(9, 0, 2)
        self._market_close_watch_time = time(15, 30, 6)
        self._watch_start_pending = False
        self._bind_widgets()
        self._init_tables()
        self._connect_signals()
        self.load_state()
        self._summary_csv_saved_count = len(self.list3)
        self.kiwoom_client = KiwoomRealtimeClient(
            self.append_buy_log,
            self.append_sell_log,
            self._handle_kiwoom_event,
        )
        self.order_manager = self._create_order_manager()
        self.kiwoom_client.order_manager = self.order_manager
        self._time_event_timer = QTimer(self)
        self._time_event_timer.timeout.connect(lambda: self.order_manager.process_time_events(datetime.now()))
        self._time_event_timer.start(1000)
        self._csv_replay_timer = QTimer(self)
        self._csv_replay_timer.timeout.connect(self._run_csv_replay_step)
        self._delayed_watch_timer = QTimer(self)
        self._delayed_watch_timer.setSingleShot(True)
        self._delayed_watch_timer.timeout.connect(self._start_watch_all_after_login)
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

        self.test_code: Optional[QLineEdit] = self._get(["test_code"], required=False)
        self.test_s: Optional[QLineEdit] = self._get(["test_s"], required=False)
        self.test_l: Optional[QLineEdit] = self._get(["test_l"], required=False)
        self.test_c: Optional[QLineEdit] = self._get(["test_c"], required=False)
        self.test_t: Optional[QLineEdit] = self._get(["test_t"], required=False)
        self.test_time: Optional[QLineEdit] = self._get(["test_time", "test_code_2"], required=False)

        self.pushButton_ini: Optional[QPushButton] = self._get(["pushButton_ini"], required=False)
        self.pushButton_reg: Optional[QPushButton] = self._get(["pushButton_reg", "pushButton_D0_1"], required=False)
        self.pushButton_reg_D1: Optional[QPushButton] = self._get(["pushButton_reg_D1"], required=False)
        self.pushButton_reg_c: Optional[QPushButton] = self._get(["pushButton_reg_c", "pushButton_D0_2"], required=False)
        self.pushButton_clear: Optional[QPushButton] = self._get(["pushButton_clear"], required=False)
        self.pushButton_Start: Optional[QPushButton] = self._get(["pushButton_Start"], required=False)
        self.pushButton_Stop: Optional[QPushButton] = self._get(["pushButton_Stop"], required=False)
        self.pushButton_demo: Optional[QPushButton] = self._get(["pushButton_demo"], required=False)
        self.test_buy: Optional[QPushButton] = self._get(["test_buy"], required=False)
        self.test_sell: Optional[QPushButton] = self._get(["test_sell"], required=False)
        self.csv_go: Optional[QPushButton] = self._get(["csv_go"], required=False)
        self.csv_stop: Optional[QPushButton] = self._get(["csv_stop"], required=False)
        self.pushButton_Save: Optional[QPushButton] = self._get(["pushButton_Save"], required=False)
        self.pushButton_End: Optional[QPushButton] = self._get(["pushButton_End"], required=False)

    def _init_tables(self) -> None:
        self._init_table(self.tableWidget_Book, LIST0_COLUMNS + ["삭제"])
        self._init_table(self.tableWidget_Buy, LIST1_COLUMNS + ["감시버튼", "삭제"])
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
            self.pushButton_reg.clicked.connect(lambda: self._run_ui_action(lambda: self.register_symbol("일반"), "종목 등록"))
        if self.pushButton_reg_D1:
            self.pushButton_reg_D1.clicked.connect(lambda: self._run_ui_action(lambda: self.register_symbol("D1"), "D1 종목 등록"))
        if self.pushButton_reg_c:
            self.pushButton_reg_c.clicked.connect(lambda: self._run_ui_action(lambda: self.register_symbol("신용"), "신용 종목 등록"))
        if self.pushButton_clear:
            self.pushButton_clear.clicked.connect(self.clear_register_inputs)
        if self.pushButton_Start:
            self.pushButton_Start.clicked.connect(self.start_watch_all)
        if self.pushButton_Stop:
            self.pushButton_Stop.clicked.connect(self.stop_watch_all)
        if self.pushButton_demo:
            self.pushButton_demo.clicked.connect(self.run_demo_watch_selection)
        if self.test_buy:
            self.test_buy.clicked.connect(self.run_test_buy_tick)
        if self.test_sell:
            self.test_sell.clicked.connect(self.run_test_sell_tick)
        if self.csv_go:
            self.csv_go.clicked.connect(self.start_csv_replay)
        if self.csv_stop:
            self.csv_stop.clicked.connect(self.stop_csv_replay)
        if self.pushButton_Save:
            self.pushButton_Save.clicked.connect(self.save_state)
        if self.pushButton_End:
            self.pushButton_End.clicked.connect(self.close)

        for edit in self.findChildren(QLineEdit):
            edit.textChanged.connect(self.mark_dirty)

    def _run_ui_action(self, action, title: str) -> None:
        try:
            action()
        except Exception as exc:
            message = f"{title} 중 오류가 발생했습니다.\n{exc}"
            if self.textEdit_reg:
                self.textEdit_reg.append(message)
                self.textEdit_reg.append(traceback.format_exc())
            QMessageBox.critical(self, title, message)

    def clear_register_inputs(self) -> None:
        for edit in (
            self.lineEdit_code,
            self.lineEdit_Buy1,
            self.lineEdit_Sell11,
            self.lineEdit_Sell12,
            self.lineEdit_Sell13,
            self.lineEdit_lose1,
        ):
            if edit is not None:
                edit.clear()

    def _create_order_manager(self) -> BuySellOrderManager:
        return BuySellOrderManager(
            self.list1,
            self.list2,
            self.list3,
            self.kiwoom_client,
            self.append_buy_log,
            self.append_sell_log,
            self._refresh_after_order_manager_update,
        )

    def _refresh_after_order_manager_update(self) -> None:
        self._save_new_trade_summaries(self._summary_csv_saved_count)
        self.mark_dirty()
        self.refresh_all_tables()

    def save_trade_logs(self) -> None:
        try:
            save_trade_logs_txt(
                self.base_dir / "logs",
                datetime.now(),
                self.textEdit_reg.toPlainText() if self.textEdit_reg else "",
                self.textEdit_Buy.toPlainText() if self.textEdit_Buy else "",
                self.textEdit_Sell.toPlainText() if self.textEdit_Sell else "",
            )
        except Exception as exc:
            self.append_sell_log(f"[로그 저장] 실패: {exc}")

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
        code = re.sub(r"[^0-9A-Za-z]", "", raw or "").upper()
        if not code:
            return ""
        return code.zfill(6) if code.isdigit() else code

    def _extract_name_hint(self, raw: str, code: str) -> Optional[str]:
        if not raw or not code:
            return None
        name = re.sub(re.escape(code), "", raw, flags=re.IGNORECASE)
        name = re.sub(r"[0-9A-Za-z]", "", name).strip()
        return name or None

    def _parse_optional_sell(self, edit: Optional[QLineEdit]) -> Optional[int]:
        text = self._line_text(edit)
        if text == "":
            return None
        value = parse_int(text)
        if value == 0:
            return None
        return value

    def _fetch_name_and_close(self, code: str, name_hint: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if stock is None:
            return None

        now = datetime.now()
        today = now.date()
        start = (today - timedelta(days=45)).strftime("%Y%m%d")
        end = today.strftime("%Y%m%d")

        name = self._fetch_ticker_name(code, name_hint) or code
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

    def _fetch_ticker_name(self, code: str, name_hint: Optional[str] = None) -> Optional[str]:
        name = self._fetch_ticker_name_from_pykrx(code)
        if name:
            return name
        name = self._find_ticker_name_in_local_rows(code)
        if name:
            return name
        if name_hint:
            return name_hint
        return self._fetch_ticker_name_from_naver(code)

    def _fetch_ticker_name_from_pykrx(self, code: str) -> Optional[str]:
        if stock is None:
            return None
        try:
            name = stock.get_market_ticker_name(code)
        except Exception:
            return None
        if isinstance(name, str) and name.strip() and name.strip() != code:
            return name.strip()
        return None

    def _find_ticker_name_in_local_rows(self, code: str) -> Optional[str]:
        for rows in (self.list0, self.list1, self.list2, self.list3):
            for row in rows:
                if str(row.get("종목코드", "")) != code:
                    continue
                name = str(row.get("종목명", "")).strip()
                if name and name != code:
                    return name
        return None

    def _fetch_ticker_name_from_naver(self, code: str) -> Optional[str]:
        url = f"https://finance.naver.com/item/main.naver?code={code}"
        request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urllib.request.urlopen(request, timeout=3) as response:
                encoding = response.headers.get_content_charset() or "utf-8"
                html = response.read().decode(encoding, errors="ignore")
        except Exception:
            return None

        match = re.search(r"<title>\s*(.*?)\s*[:|]", html, re.IGNORECASE | re.DOTALL)
        if not match:
            return None
        name = re.sub(r"\s+", " ", match.group(1)).strip()
        return name if name and name != code else None

    def register_symbol(self, buy_type: str) -> None:
        code_text = self._line_text(self.lineEdit_code)
        code = self._normalize_code(code_text)
        name_hint = self._extract_name_hint(code_text, code)
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

        market = self._fetch_name_and_close(code, name_hint)
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
            "_실시간갱신완료": False,
        }
        self.list0.append(row0)
        self._priority_selection_done = False

        self.mark_dirty()
        self.refresh_all_tables()

    def on_initialize(self) -> None:
        for edit in self.findChildren(QLineEdit):
            edit.clear()

    def start_watch_all(self) -> None:
        if not self.kiwoom_client.logged_in:
            self.kiwoom_client.login()
            return
        self._start_watch_all_after_login()

    def _start_watch_all_after_login(self) -> None:
        if self._is_after_market_watch_time():
            self._watch_start_pending = False
            if self._delayed_watch_timer.isActive():
                self._delayed_watch_timer.stop()
            self.append_buy_log("[감시] 정규장 감시 가능 시간이 지나 자동 감시/주문을 시작하지 않았습니다.")
            self.append_sell_log("[감시] 정규장 감시 가능 시간이 지나 자동 감시/주문을 시작하지 않았습니다.")
            return
        if self._should_delay_market_open_watch():
            self._schedule_market_open_watch()
            return
        self._watch_start_pending = False
        self._buy_watch_active = True
        self._priority_selection_done = False
        for row in self.list0:
            row["감시 상태"] = "감시 중"
            row.pop("감시상태", None)
        self._clear_intraday_quote_fields(self.list0)
        self._clear_intraday_quote_fields(
            row
            for row in self.list1
            if row.get("매수주문상태", "미주문") == "미주문"
        )
        for row_idx in range(len(self.list1)):
            self.order_manager.start_buy_watch(row_idx)
        for row in self.list2:
            row["감시 상태"] = "감시 중"
            row.pop("감시상태", None)
        self.append_buy_log("[감시] 매수 감시를 시작했습니다.")
        self.append_sell_log("[감시] 매도 감시를 시작했습니다.")
        self.kiwoom_client.start_quote_watch(self._kiwoom_list0_rows, self._kiwoom_realtime_rows)
        self.mark_dirty()
        self.refresh_all_tables()

    def _clear_intraday_quote_fields(self, rows) -> None:
        for row in rows:
            row["시가"] = None
            row["현재가"] = None
            row["저가"] = None
            row["실시간 거래량"] = None
            row["_실시간갱신완료"] = False
            row.pop("직전저가", None)

    def stop_watch_all(self) -> None:
        self._watch_start_pending = False
        if self._delayed_watch_timer.isActive():
            self._delayed_watch_timer.stop()
        self._buy_watch_active = False
        self.kiwoom_client.stop_quote_watch()
        for row_idx in range(len(self.list1)):
            self.order_manager.stop_buy_watch(row_idx)
        for row in self.list2:
            row["감시 상태"] = "감시 중지"
            row.pop("감시상태", None)
        self.append_buy_log("[감시] 매수 감시를 중지했습니다.")
        self.append_sell_log("[감시] 매도 감시를 중지했습니다.")
        self.mark_dirty()
        self.refresh_all_tables()

    def _should_delay_market_open_watch(self) -> bool:
        now = datetime.now()
        return now.time() < self._market_open_watch_time

    def _is_after_market_watch_time(self) -> bool:
        now = datetime.now()
        return now.time() > self._market_close_watch_time

    def _schedule_market_open_watch(self) -> None:
        now = datetime.now()
        target = datetime.combine(now.date(), self._market_open_watch_time)
        delay_ms = max(0, int((target - now).total_seconds() * 1000))
        if not self._watch_start_pending:
            self.append_buy_log("[감시] 정규장 시가 반영 대기: 09:00:02부터 감시를 시작합니다.")
            self.append_sell_log("[감시] 정규장 시가 반영 대기: 09:00:02부터 감시를 시작합니다.")
        self._watch_start_pending = True
        self._delayed_watch_timer.start(delay_ms)

    def _row_is_watching(self, row: Dict[str, Any]) -> bool:
        return (
            row.get("감시상태", row.get("감시 상태", "감시 중")) == "감시 중"
            or row.get("감시 상태", row.get("감시상태", "감시 중")) == "감시 중"
        )

    def _kiwoom_list0_rows(self) -> List[Dict[str, Any]]:
        return [
            row
            for row in self.list0
            if not row.get("모의시간고정") and self._row_is_watching(row)
        ]

    def _kiwoom_realtime_rows(self) -> List[Dict[str, Any]]:
        return [
            row
            for row in (self.list1 + self.list2)
            if not row.get("모의시간고정") and self._row_is_watching(row)
        ]

    def _handle_kiwoom_event(self, quote: Optional[RealtimeQuote] = None) -> None:
        if quote is None:
            self._start_watch_all_after_login()
            return
        self._update_list0_quote(quote)
        self._update_list1_pending_quote(quote)
        self._select_list0_candidates_from_kiwoom()
        if self._should_dispatch_quote_to_order_manager(quote.code):
            self.order_manager.handle_realtime_quote(quote)
        self.mark_dirty()
        self.refresh_all_tables()

    def _should_dispatch_quote_to_order_manager(self, code: str) -> bool:
        if self._find_row_by_code(self.list2, code) is not None:
            return True
        row = self._find_row_by_code(self.list1, code)
        if row is None:
            return False
        return row.get("매수주문상태", "미주문") != "미주문"

    def _update_list0_quote(self, quote: RealtimeQuote) -> None:
        row = self._find_row_by_code(self.list0, quote.code)
        if row is None:
            return
        if quote.open_price > 0:
            row["시가"] = quote.open_price
        row["현재가"] = quote.current_price
        row["저가"] = quote.low_price
        row["실시간 거래량"] = quote.accumulated_volume
        row["_실시간갱신완료"] = True

    def _update_list1_pending_quote(self, quote: RealtimeQuote) -> None:
        row = self._find_row_by_code(self.list1, quote.code)
        if row is None or row.get("매수주문상태", "미주문") != "미주문":
            return
        if quote.open_price > 0:
            row["시가"] = quote.open_price
        row["현재가"] = quote.current_price
        row["저가"] = quote.low_price
        row["실시간 거래량"] = quote.accumulated_volume
        row["_실시간갱신완료"] = True

    def _select_list0_candidates_from_kiwoom(self) -> None:
        if not self.list0:
            return

        d1_rows = [
            row
            for row in self.list0
            if (
                str(row.get("매수 타입", "")) == "D1"
                and self._row_is_watching(row)
                and row.get("_실시간갱신완료")
                and to_int(row.get("시가")) > 0
                and to_int(row.get("실시간 거래량")) > 0
            )
        ]
        if d1_rows:
            open_prices = {str(row.get("종목코드", "")): to_int(row.get("시가")) for row in d1_rows}
            result = select_watch_candidates(d1_rows, 0, open_prices)
            self._move_selected_list0_rows(result, log_stopped=False)
            selected_codes = set(result.selected_codes)
            for row in d1_rows:
                if str(row.get("종목코드", "")) not in selected_codes:
                    row["감시상태"] = "감시 중지"
                    row["감시 상태"] = "감시 중지"
            if self.textEdit_reg:
                for message in result.stopped_messages:
                    self.textEdit_reg.append(message)

        if self._priority_selection_done:
            return

        priority_rows = [
            row
            for row in self.list0
            if str(row.get("매수 타입", "")) != "D1" and self._row_is_watching(row)
        ]
        if not priority_rows:
            return
        if any(
            not row.get("_실시간갱신완료")
            or to_int(row.get("시가")) <= 0
            or to_int(row.get("실시간 거래량")) <= 0
            for row in priority_rows
        ):
            return

        priority_limit = self._line_int(self.lineEdit_rank)
        open_prices = {str(row.get("종목코드", "")): to_int(row.get("시가")) for row in priority_rows}
        result = select_watch_candidates(priority_rows, priority_limit, open_prices)
        self._priority_selection_done = True
        self._move_selected_list0_rows(result, log_stopped=True)

    def _move_selected_list0_rows(self, result, log_stopped: bool) -> None:
        if result.selected_rows:
            start_idx = len(self.list1)
            self.list1.extend(result.selected_rows)
            selected_codes = set(result.selected_codes)
            self.list0 = [
                row
                for row in self.list0
                if str(row.get("종목코드", "")) not in selected_codes
            ]
            for row in result.selected_rows:
                name = row.get("종목명", row.get("종목코드", ""))
                rank = row.get("우선순위", "")
                rank_text = "D1" if rank == 0 else f"우선순위 {rank}"
                if self.textEdit_reg:
                    self.textEdit_reg.append(f"{name} 감시 등록: {rank_text}")
            if self._buy_watch_active:
                for row_idx in range(start_idx, len(self.list1)):
                    self.order_manager.start_buy_watch(row_idx)
        if log_stopped:
            selected_codes = set(result.selected_codes)
            for row in self.list0:
                if str(row.get("매수 타입", "")) != "D1" and str(row.get("종목코드", "")) not in selected_codes:
                    row["감시상태"] = "감시 중지"
                    row["감시 상태"] = "감시 중지"
            if self.textEdit_reg:
                for message in result.stopped_messages:
                    self.textEdit_reg.append(message)

    def start_csv_replay(self) -> None:
        if not self._csv_replay_rows or self._csv_replay_index >= len(self._csv_replay_rows):
            self._load_csv_replay()
        if not self._csv_replay_rows:
            return
        if self._time_event_timer.isActive():
            self._time_event_timer.stop()
        self.kiwoom_client.mock_orders = True
        if not self._csv_replay_timer.isActive():
            self._csv_replay_timer.start(2000)
        if self.textEdit_reg:
            self.textEdit_reg.append(f"[CSV] 리플레이 시작: {self.replay_csv_path.name} ({self._csv_replay_index + 1}/{len(self._csv_replay_rows)})")
        self._run_csv_replay_step()

    def stop_csv_replay(self) -> None:
        if self._csv_replay_timer.isActive():
            self._csv_replay_timer.stop()
        self.kiwoom_client.mock_orders = False
        if self.textEdit_reg:
            self.textEdit_reg.append(f"[CSV] 리플레이 중지: {self._csv_replay_index}/{len(self._csv_replay_rows)}")

    def _load_csv_replay(self) -> None:
        self._csv_replay_rows = []
        self._csv_replay_index = 0
        if not self.replay_csv_path.exists():
            QMessageBox.warning(self, "CSV 리플레이", f"CSV 파일을 찾을 수 없습니다.\n{self.replay_csv_path}")
            return
        with self.replay_csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            self._csv_replay_rows = list(csv.DictReader(handle))

    def _run_csv_replay_step(self) -> None:
        if self._csv_replay_index >= len(self._csv_replay_rows):
            self._csv_replay_timer.stop()
            self.kiwoom_client.mock_orders = False
            if self.textEdit_reg:
                self.textEdit_reg.append("[CSV] 리플레이 완료")
            return

        replay_row = self._csv_replay_rows[self._csv_replay_index]
        self._csv_replay_index += 1
        quote = self._quote_from_csv_row(replay_row)
        action = str(replay_row.get("action", "")).strip().lower()
        self._mark_csv_mock_rows(True)
        try:
            self._handle_kiwoom_event(quote)
            self._start_csv_pending_buy_orders()
            if "buy_fill" in action:
                self._fill_csv_buy_if_needed(quote)
            self.order_manager.process_time_events(quote.event_time)
            self._confirm_test_sell_cancel_if_needed(quote.code)
            self.order_manager.process_time_events(quote.event_time)
            self._confirm_test_sell_cancel_if_needed(quote.code)
            self._fill_test_market_sell_if_needed(self._test_quote_from_realtime(quote))

            row = self._find_row_by_code(self.list2, quote.code)
            test_quote = self._test_quote_from_realtime(quote)
            if row is not None:
                self._fill_test_sell_limits(row, test_quote)
            row = self._find_row_by_code(self.list2, quote.code)
            if row is not None:
                self._fill_test_stop_loss(row, test_quote)
        finally:
            self._mark_csv_mock_rows(False)

        if self.textEdit_reg:
            self.textEdit_reg.append(
                f"[CSV] {quote.event_time.strftime('%H:%M:%S')} {quote.code} "
                f"시가 {quote.open_price:,} 현재가 {quote.current_price:,} 저가 {quote.low_price:,}"
            )
        self.mark_dirty()
        self.refresh_all_tables()

    def _quote_from_csv_row(self, row: Dict[str, str]) -> RealtimeQuote:
        raw_time = str(row.get("time", "")).strip()
        parsed_time = None
        for time_format in ("%H:%M:%S", "%H:%M"):
            try:
                parsed_time = datetime.strptime(raw_time, time_format).time()
                break
            except ValueError:
                continue
        if parsed_time is None:
            parsed_time = datetime.now().time()
        today = datetime.now().date()
        return RealtimeQuote(
            code=self._normalize_code(str(row.get("code", ""))),
            current_price=parse_int(str(row.get("current", ""))),
            low_price=parse_int(str(row.get("low", ""))),
            accumulated_volume=parse_int(str(row.get("volume", ""))),
            event_time=datetime.combine(today, parsed_time),
            open_price=parse_int(str(row.get("open", ""))),
        )

    def _test_quote_from_realtime(self, quote: RealtimeQuote) -> TestQuote:
        return TestQuote(
            code=quote.code,
            open_price=quote.open_price,
            low_price=quote.low_price,
            current_price=quote.current_price,
            accumulated_volume=quote.accumulated_volume,
            event_time=quote.event_time,
        )

    def _mark_csv_mock_rows(self, enabled: bool) -> None:
        for row in self.list1 + self.list2:
            if enabled:
                row["_모의테스트"] = True
            else:
                row.pop("_모의테스트", None)

    def _start_csv_pending_buy_orders(self) -> None:
        for row_idx, row in enumerate(list(self.list1)):
            if row.get("매수주문상태", "미주문") == "미주문":
                row["_모의테스트"] = True
                self.order_manager.start_buy_watch(row_idx)

    def _fill_csv_buy_if_needed(self, quote: RealtimeQuote) -> None:
        row = self._find_row_by_code(self.list1, quote.code)
        if row is None:
            return
        buy_qty = to_int(row.get("매수 수량"))
        if buy_qty <= 0:
            return
        row["시가"] = quote.open_price
        row["현재가"] = quote.current_price
        row["저가"] = quote.low_price
        row["실시간 거래량"] = quote.accumulated_volume
        row["매수주문번호"] = "CSV_BUY"
        row["매수주문상태"] = "전량체결"
        self.append_buy_log(f"[CSV] {self._row_name(row)}: {quote.current_price:,}원/{buy_qty}주 매수 체결")
        self.order_manager.handle_buy_fill(
            BuyFillEvent(
                code=quote.code,
                order_no="CSV_BUY",
                fill_qty=buy_qty,
                cumulative_filled_qty=buy_qty,
                unfilled_qty=0,
                fill_price=quote.current_price,
                event_time=quote.event_time,
            )
        )

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

    def run_test_buy_tick(self) -> None:
        quote = self._read_test_quote()
        if quote is None:
            return

        row = self._find_row_by_code(self.list1, quote.code)
        if row is None:
            self.append_buy_log(f"[매수 테스트] {quote.code}: list1 매수 감시 종목이 아닙니다.")
            return

        row["시가"] = quote.open_price
        row["현재가"] = quote.current_price
        row["저가"] = quote.low_price
        row["실시간 거래량"] = quote.accumulated_volume
        self.order_manager.handle_realtime_quote(
            RealtimeQuote(
                code=quote.code,
                current_price=quote.current_price,
                low_price=quote.low_price,
                accumulated_volume=quote.accumulated_volume,
                event_time=quote.event_time,
            )
        )
        if row.get("감시상태") != "감시 중":
            self.mark_dirty()
            self.refresh_all_tables()
            return

        buy_price = to_int(row.get("매수가"))
        buy_qty = to_int(row.get("매수 수량"))
        if buy_price <= 0 or buy_qty <= 0:
            self.append_buy_log(f"[매수 테스트] {self._row_name(row)}: 매수가/수량 오류")
            return
        if quote.current_price > buy_price:
            self.append_buy_log(f"[매수 테스트] {self._row_name(row)}: 감시중..")
            self.mark_dirty()
            self.refresh_all_tables()
            return

        row["매수주문번호"] = "TEST_BUY"
        row["매수주문상태"] = "전량체결"
        self.append_buy_log(f"[매수 테스트] {self._row_name(row)}: {buy_price:,}원/{buy_qty}주 매수 체결")
        self.order_manager.handle_buy_fill(
            BuyFillEvent(
                code=quote.code,
                order_no="TEST_BUY",
                fill_qty=buy_qty,
                cumulative_filled_qty=buy_qty,
                unfilled_qty=0,
                fill_price=buy_price,
                event_time=quote.event_time,
            )
        )
        test_sell_row = self._find_row_by_code(self.list2, quote.code)
        if test_sell_row is not None:
            test_sell_row["모의시간고정"] = quote.event_time.isoformat()
            test_sell_row["매도전략상태"] = "일반지정가"
            test_sell_row["청산사유"] = ""
            self.mark_dirty()
            self.refresh_all_tables()

    def run_test_sell_tick(self) -> None:
        quote = self._read_test_quote()
        if quote is None:
            return

        row = self._find_row_by_code(self.list2, quote.code)
        if row is None:
            self.append_sell_log(f"[매도 테스트] {quote.code}: list2 매도 감시 종목이 아닙니다.")
            return

        before_summary_count = len(self.list3)
        row["_모의테스트"] = True
        row["모의시간고정"] = quote.event_time.isoformat()
        try:
            self.order_manager.handle_sell_realtime_quote(
                RealtimeQuote(
                    code=quote.code,
                    current_price=quote.current_price,
                    low_price=quote.low_price,
                    accumulated_volume=quote.accumulated_volume,
                    event_time=quote.event_time,
                )
            )
            self.order_manager.process_time_events(quote.event_time)
            self._confirm_test_sell_cancel_if_needed(quote.code)
            self._fill_test_market_sell_if_needed(quote)
        finally:
            for rows in (self.list1, self.list2, self.list3):
                for item in rows:
                    item.pop("_모의테스트", None)

        row = self._find_row_by_code(self.list2, quote.code)
        if row is not None:
            self._fill_test_sell_limits(row, quote)

        row = self._find_row_by_code(self.list2, quote.code)
        if row is not None:
            self._fill_test_stop_loss(row, quote)

        self._save_new_trade_summaries(before_summary_count)
        self.mark_dirty()
        self.refresh_all_tables()

    def _confirm_test_sell_cancel_if_needed(self, code: str) -> None:
        row = self._find_row_by_code(self.list2, code)
        if row is None:
            return
        if row.get("매도전략상태") not in {"지정가취소요청", "재배치취소요청", "재배치1차취소요청"}:
            return
        self.order_manager.handle_sell_cancel_confirmed(
            SellCancelConfirmedEvent(
                code=code,
                remaining_holding_qty=to_int(row.get("잔여 수량")),
            )
        )
    def _fill_test_market_sell_if_needed(self, quote: "TestQuote") -> None:
        row = self._find_row_by_code(self.list2, quote.code)
        if row is None:
            return
        market_qty = min(to_int(row.pop("_모의시장가수량", 0)), to_int(row.get("잔여 수량")))
        if market_qty <= 0:
            return
        remaining_qty = max(0, to_int(row.get("잔여 수량")) - market_qty)
        self.append_sell_log(f"[매도 테스트] {self._row_name(row)}: 시장가 {quote.current_price:,}원/{market_qty}주 체결")
        self.order_manager.handle_sell_fill(
            SellFillEvent(
                code=quote.code,
                order_no=str(row.get("시장가주문번호") or "TEST_MARKET"),
                sell_level=0,
                fill_qty=market_qty,
                fill_price=quote.current_price,
                remaining_holding_qty=remaining_qty,
                event_time=quote.event_time,
            )
        )
    def _fill_test_sell_limits(self, row: Dict[str, Any], quote: "TestQuote") -> None:
        for level in (1, 2, 3):
            row = self._find_row_by_code(self.list2, quote.code)
            if row is None:
                return

            price = to_int(row.get(f"매도가{level}"))
            qty = self._remaining_sell_level_qty(row, level)
            if price <= 0 or qty <= 0 or quote.current_price < price:
                continue

            remaining_qty = max(0, to_int(row.get("잔여 수량")) - qty)
            self.append_sell_log(f"[매도 테스트] {self._row_name(row)}: 매도가{level} {price:,}원/{qty}주 체결")
            self.order_manager.handle_sell_fill(
                SellFillEvent(
                    code=quote.code,
                    order_no=str(row.get(f"매도가{level}주문번호") or f"TEST_SELL{level}"),
                    sell_level=level,
                    fill_qty=qty,
                    fill_price=price,
                    remaining_holding_qty=remaining_qty,
                    event_time=quote.event_time,
                )
            )

    def _fill_test_stop_loss(self, row: Dict[str, Any], quote: "TestQuote") -> None:
        stop_price = to_int(row.get("손절가"))
        holding_qty = to_int(row.get("잔여 수량"))
        if stop_price <= 0 or holding_qty <= 0 or quote.current_price > stop_price:
            return

        self.append_sell_log(f"[매도 테스트] {self._row_name(row)}: 손절가 도달, {holding_qty}주 시장가 청산")
        self.order_manager.handle_sell_fill(
            SellFillEvent(
                code=quote.code,
                order_no="TEST_MARKET",
                sell_level=0,
                fill_qty=holding_qty,
                fill_price=quote.current_price,
                remaining_holding_qty=0,
                event_time=quote.event_time,
            )
        )

    def _save_new_trade_summaries(self, before_summary_count: int) -> None:
        if len(self.list3) <= before_summary_count:
            return
        new_rows = self.list3[before_summary_count:]
        append_trade_summaries_csv(self.summary_csv_path, new_rows)
        self._summary_csv_saved_count = len(self.list3)
        self.append_sell_log(f"[매매요약] 매매 결과 CSV 저장: {self.summary_csv_path.name}")

    def _remaining_sell_level_qty(self, row: Dict[str, Any], level: int) -> int:
        ordered_qty = to_int(row.get(f"매도가{level}주문수량"))
        if ordered_qty <= 0:
            ordered_qty = to_int(row.get("잔여 수량"))
        filled_qty = to_int(row.get(f"매도가{level}체결수량"))
        return max(0, ordered_qty - filled_qty)

    def _find_row_by_code(self, rows: List[Dict[str, Any]], code: str) -> Optional[Dict[str, Any]]:
        for row in rows:
            if str(row.get("종목코드", "")) == str(code):
                return row
        return None

    def _row_name(self, row: Dict[str, Any]) -> str:
        return str(row.get("종목명") or row.get("종목코드") or "")

    def _read_test_quote(self) -> Optional["TestQuote"]:
        code = self._normalize_code(self._line_text(self.test_code))
        if not code:
            QMessageBox.warning(self, "모의 테스트 입력 오류", "종목 코드를 입력하세요.")
            return None

        open_price = self._line_int(self.test_s)
        low_price = self._line_int(self.test_l)
        current_price = self._line_int(self.test_c)
        volume = self._line_int(self.test_t)
        if open_price <= 0 or low_price <= 0 or current_price <= 0:
            QMessageBox.warning(self, "모의 테스트 입력 오류", "시가/저가/현재가를 입력하세요.")
            return None

        return TestQuote(
            code=code,
            open_price=open_price,
            low_price=low_price,
            current_price=current_price,
            accumulated_volume=volume,
            event_time=self._read_test_datetime(),
        )

    def _read_test_datetime(self) -> datetime:
        raw_time = self._line_text(self.test_time)
        if not raw_time:
            return datetime.now()
        parsed_time = None
        for time_format in ("%H:%M:%S", "%H:%M"):
            try:
                parsed_time = datetime.strptime(raw_time, time_format).time()
                break
            except ValueError:
                continue
        if parsed_time is None:
            QMessageBox.warning(self, "모의 테스트 입력 오류", "현재 시간은 13:20 또는 13:20:30 형식으로 입력하세요.")
            return datetime.now()
        if raw_time == "15:25":
            parsed_time = time(15, 25, 30)
        today = datetime.now().date()
        return datetime.combine(today, parsed_time)

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

            delete_btn = QPushButton("삭제")
            delete_btn.clicked.connect(lambda _checked=False, idx=row_idx: self.delete_buy_row(idx))
            table.setCellWidget(row_idx, len(LIST1_COLUMNS) + 1, delete_btn)

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

    def delete_buy_row(self, row_idx: int) -> None:
        if not (0 <= row_idx < len(self.list1)):
            return

        row = self.list1.pop(row_idx)
        status = row.get("매수주문상태", "")
        if status in {
            BUY_STATUS_REQUESTED,
            BUY_STATUS_ACCEPTED,
            BUY_STATUS_PARTIAL_WAIT,
            BUY_STATUS_FILLED,
            BUY_STATUS_CANCEL_REQUESTED,
        }:
            self.kiwoom_client.stop_realtime_watch(row)
            self.kiwoom_client.cancel_buy_order(row)

        self.mark_dirty()
        self.refresh_all_tables()

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
        self.save_trade_logs()
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
            self._clear_saved_pending_sell_order_numbers()
            self._clear_intraday_quote_fields(self.list0)
            self._clear_intraday_quote_fields(
                row
                for row in self.list1
                if row.get("매수주문상태", "미주문") == "미주문"
            )
            rank = state["rank"]
            if self.lineEdit_rank:
                self.lineEdit_rank.setText(rank)
            if self.textEdit_reg and rank:
                self.textEdit_reg.setPlainText(f"우선 순위: {rank}")
            self._dirty = False
        except Exception as exc:
            QMessageBox.warning(self, "불러오기 실패", f"저장 파일을 읽는 중 오류가 발생했습니다.\n{exc}")

    def _clear_saved_pending_sell_order_numbers(self) -> None:
        for row in self.list2:
            changed = False
            for key in (
                "매도가1주문번호",
                "매도가2주문번호",
                "매도가3주문번호",
                "시장가주문번호",
            ):
                values = [
                    value.strip()
                    for value in str(row.get(key, "")).split(",")
                    if value.strip() and not value.strip().startswith("PENDING_")
                ]
                next_value = ",".join(values)
                if str(row.get(key, "")) != next_value:
                    row[key] = next_value
                    changed = True
            if changed:
                row["매도주문번호들"] = ",".join(
                    order_no
                    for order_no in [
                        str(row.get("매도가1주문번호", "")),
                        str(row.get("매도가2주문번호", "")),
                        str(row.get("매도가3주문번호", "")),
                    ]
                    if order_no
                )

    def closeEvent(self, event) -> None:  # type: ignore[override]
        self.save_trade_logs()
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
