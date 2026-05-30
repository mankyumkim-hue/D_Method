from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from typing import Any, Callable, Dict, List, Optional, Protocol, Sequence, Tuple

from trade_summary import make_trade_summary_row


BUY_STATUS_NOT_ORDERED = "미주문"
BUY_STATUS_REQUESTED = "주문요청"
BUY_STATUS_ACCEPTED = "접수"
BUY_STATUS_PARTIAL_WAIT = "부분체결대기"
BUY_STATUS_CANCEL_REQUESTED = "취소요청"
BUY_STATUS_CANCEL_DONE = "취소완료"
BUY_STATUS_FILLED = "전량체결"
BUY_STATUS_FAILED = "주문실패"

SELL_STRATEGY_NORMAL = "일반지정가"
SELL_STRATEGY_PROFIT = "익절추적"
SELL_STRATEGY_RELOCATED = "매도재배치"
SELL_STRATEGY_RELOCATED_PROFIT = "재배치익절추적"
SELL_STATUS_RELOCATION_CANCEL_REQUESTED = "재배치취소요청"
SELL_STATUS_RELOCATION_LEVEL1_CANCEL_REQUESTED = "재배치1차취소요청"
SELL_STATUS_CANCEL_REQUESTED = "지정가취소요청"
SELL_STATUS_MARKET_REQUESTED = "시장가청산요청"
SELL_STATUS_DONE = "청산완료"

WATCH_STATUS_ON = "감시 중"
WATCH_STATUS_OFF = "감시 중지"

BUY_RETRY_LIMIT = 1
PARTIAL_FILL_WAIT_SECONDS = 6


class KiwoomOrderClient(Protocol):
    """order_manager의 주문 로직이 기대하는 키움 API 어댑터 인터페이스."""

    def send_buy_order(self, row: Dict[str, Any]) -> bool:
        """list1 row의 매수가/매수 수량으로 매수 주문을 요청한다."""

    def cancel_buy_order(self, row: Dict[str, Any]) -> bool:
        """row의 매수주문번호 기준으로 미체결 매수 잔량 취소를 요청한다."""

    def send_sell_order(self, row: Dict[str, Any], price: int, qty: int) -> Optional[str]:
        """list2 row 기준으로 매도 주문을 요청하고, 접수된 주문번호를 반환한다."""

    def cancel_sell_orders(self, row: Dict[str, Any]) -> bool:
        """row의 잔여 지정가 매도 주문을 취소 요청한다."""

    def cancel_sell_order(self, row: Dict[str, Any], order_no: str) -> bool:
        """row의 특정 지정가 매도 주문을 취소 요청한다."""

    def send_market_sell_order(self, row: Dict[str, Any], qty: int) -> Optional[str]:
        """row의 잔여 보유수량을 시장가로 매도 요청하고, 접수된 주문번호를 반환한다."""

    def start_realtime_watch(self, row: Dict[str, Any]) -> None:
        """현재가, 저가, 누적 거래량 실시간 감시를 시작한다."""

    def stop_realtime_watch(self, row: Dict[str, Any]) -> None:
        """실시간 감시를 중단한다."""


@dataclass
class BuyFillEvent:
    code: str
    order_no: str
    cumulative_filled_qty: int
    unfilled_qty: int
    event_time: datetime


@dataclass
class BuyOrderAcceptedEvent:
    code: str
    order_no: str


@dataclass
class BuyOrderFailedEvent:
    code: str
    reason: str


@dataclass
class BuyCancelConfirmedEvent:
    code: str
    order_no: str
    unfilled_qty: int


@dataclass
class SellFillEvent:
    code: str
    order_no: str
    sell_level: int
    fill_qty: int
    fill_price: int
    remaining_holding_qty: int
    event_time: datetime


@dataclass
class SellCancelConfirmedEvent:
    code: str
    remaining_holding_qty: int


@dataclass
class MarketSellAcceptedEvent:
    code: str
    order_no: str


@dataclass
class RealtimeQuote:
    code: str
    current_price: int
    low_price: int
    accumulated_volume: int
    event_time: datetime


@dataclass
class OrderRuntimeState:
    retry_count_by_code: Dict[str, int] = field(default_factory=dict)
    first_fill_time_by_code: Dict[str, datetime] = field(default_factory=dict)


def to_int(value: Any) -> int:
    try:
        return int(float(str(value).replace(",", "").replace("원", "").strip()))
    except (TypeError, ValueError):
        return 0


def to_float(value: Any) -> float:
    try:
        return float(str(value).replace(",", "").replace("원", "").strip())
    except (TypeError, ValueError):
        return 0.0


def round_half_up(value: float) -> int:
    return math.floor(value + 0.5)


def get_krx_tick_size(price: Any) -> int:
    price_value = to_float(price)
    if price_value < 2000:
        return 1
    if price_value < 5000:
        return 5
    if price_value < 20000:
        return 10
    if price_value < 50000:
        return 50
    if price_value < 200000:
        return 100
    if price_value < 500000:
        return 500
    return 1000


def ceil_to_krx_tick(price: Any) -> int:
    price_value = to_float(price)
    if price_value <= 0:
        return 0
    tick_size = get_krx_tick_size(price_value)
    return int(math.ceil(price_value / tick_size) * tick_size)


def floor_to_krx_tick(price: Any) -> int:
    price_value = to_float(price)
    if price_value <= 0:
        return 0
    tick_size = get_krx_tick_size(price_value)
    return int(math.floor(price_value / tick_size) * tick_size)


def previous_krx_tick(price: Any) -> int:
    price_value = to_float(price)
    if price_value <= 1:
        return 0
    return floor_to_krx_tick(price_value - 1)


def is_valid_price(value: Any) -> bool:
    return to_int(value) >= 1


def build_sell_slices(holding_qty: int, sell_prices: Sequence[Any]) -> List[Tuple[int, int]]:
    """매도가1~3과 보유수량으로 실제 주문할 (가격, 수량) 목록을 만든다."""
    valid_prices = [ceil_to_krx_tick(price) for price in sell_prices if is_valid_price(price)]
    if holding_qty <= 0 or not valid_prices:
        return []

    if len(valid_prices) == 1:
        quantities = [holding_qty]
    elif len(valid_prices) == 2:
        first_qty = round_half_up(holding_qty / 2)
        quantities = [first_qty, holding_qty - first_qty]
    else:
        split_qty = round_half_up(holding_qty / 3)
        quantities = [split_qty, split_qty, holding_qty - (split_qty * 2)]
        valid_prices = valid_prices[:3]

    return [
        (price, qty)
        for price, qty in zip(valid_prices, quantities)
        if qty > 0
    ]


class BuySellOrderManager:
    """list1/list2 행을 직접 갱신하는 매수/매도 주문 상태 관리자."""

    def __init__(
        self,
        list1: List[Dict[str, Any]],
        list2: List[Dict[str, Any]],
        list3: List[Dict[str, Any]],
        kiwoom: KiwoomOrderClient,
        buy_log: Callable[[str], None],
        sell_log: Callable[[str], None],
        refresh_tables: Callable[[], None],
    ) -> None:
        self.list1 = list1
        self.list2 = list2
        self.list3 = list3
        self.kiwoom = kiwoom
        self.buy_log = buy_log
        self.sell_log = sell_log
        self.refresh_tables = refresh_tables
        self.runtime = OrderRuntimeState()

    def start_buy_watch(self, row_idx: int) -> None:
        row = self._row_by_index(row_idx)
        if row is None:
            return

        status = row.get("매수주문상태", BUY_STATUS_NOT_ORDERED)
        if status == BUY_STATUS_CANCEL_REQUESTED:
            self.buy_log(f"{self._name(row)}: 취소 처리 중")
            return
        if status in {
            BUY_STATUS_REQUESTED,
            BUY_STATUS_ACCEPTED,
            BUY_STATUS_PARTIAL_WAIT,
            BUY_STATUS_FILLED,
        }:
            self.buy_log(f"{self._name(row)}: 이미 매수 주문 처리 중")
            return

        row["감시상태"] = WATCH_STATUS_ON
        row["매수주문상태"] = BUY_STATUS_REQUESTED
        row["취소사유"] = ""
        if not self.kiwoom.send_buy_order(row):
            self._handle_buy_order_request_failed(row, "매수 주문 요청 실패")
            return

        self.refresh_tables()

    def stop_buy_watch(self, row_idx: int, reason: str = "사용자 감시 중지") -> None:
        row = self._row_by_index(row_idx)
        if row is None:
            return

        status = row.get("매수주문상태", BUY_STATUS_NOT_ORDERED)
        row["감시상태"] = WATCH_STATUS_OFF
        row["취소사유"] = reason
        self.kiwoom.stop_realtime_watch(row)

        if status in {BUY_STATUS_NOT_ORDERED, BUY_STATUS_FAILED, BUY_STATUS_CANCEL_DONE}:
            self._reset_order_after_user_cancel(row)
            self.refresh_tables()
            return

        if status == BUY_STATUS_CANCEL_REQUESTED:
            self.buy_log(f"{self._name(row)}: 취소 처리 중")
            self.refresh_tables()
            return

        self._request_buy_cancel(row, reason)
        self.refresh_tables()

    def handle_buy_order_accepted(self, event: BuyOrderAcceptedEvent) -> None:
        row = self._find_list1_row(event.code)
        if row is None:
            return

        row["매수주문번호"] = event.order_no
        row["매수주문상태"] = BUY_STATUS_ACCEPTED
        row["매수체결수량"] = to_int(row.get("매수체결수량"))
        row["매수미체결수량"] = to_int(row.get("매수 수량"))
        self.kiwoom.start_realtime_watch(row)
        self.buy_log(f"{self._name(row)}: 매수 주문 접수")
        self.refresh_tables()

    def handle_buy_order_failed(self, event: BuyOrderFailedEvent) -> None:
        row = self._find_list1_row(event.code)
        if row is None:
            return

        retry_count = self.runtime.retry_count_by_code.get(event.code, 0)
        if retry_count < BUY_RETRY_LIMIT:
            self.runtime.retry_count_by_code[event.code] = retry_count + 1
            row["매수주문상태"] = BUY_STATUS_REQUESTED
            self.buy_log(f"{self._name(row)}: 매수 주문 실패, 1회 재시도")
            if not self.kiwoom.send_buy_order(row):
                self._handle_buy_order_request_failed(row, "매수 주문 재요청 실패")
            self.refresh_tables()
            return

        row["매수주문상태"] = BUY_STATUS_FAILED
        row["감시상태"] = WATCH_STATUS_OFF
        row["취소사유"] = event.reason
        self.kiwoom.stop_realtime_watch(row)
        self.buy_log(f"{self._name(row)}: 매수 주문 실패 - {event.reason}")
        self.refresh_tables()

    def handle_realtime_quote(self, quote: RealtimeQuote) -> None:
        row = self._find_list1_row(quote.code)
        if row is None:
            self.handle_sell_realtime_quote(quote)
            return

        row["현재가"] = quote.current_price
        row["저가"] = quote.low_price
        row["실시간 거래량"] = quote.accumulated_volume

        if to_int(row.get("매수체결수량")) > 0:
            self.refresh_tables()
            return

        cancel_reason = self._buy_wait_cancel_reason(row, quote.event_time)
        if cancel_reason:
            self.buy_log(f"{self._name(row)}: {cancel_reason}")
            self._request_buy_cancel(row, cancel_reason)

        self.refresh_tables()

    def handle_sell_realtime_quote(self, quote: RealtimeQuote) -> None:
        row = self._find_list2_row(quote.code)
        if row is None:
            return

        self._ensure_sell_level_metadata(row)
        row["현재가"] = quote.current_price
        row["저가"] = quote.low_price
        self._request_sell_relocation_if_needed(row)
        self._update_relocated_stop_loss_from_low(row)
        if row.get("매도전략상태") in {SELL_STRATEGY_PROFIT, SELL_STRATEGY_RELOCATED_PROFIT}:
            self._request_profit_market_liquidation_if_needed(row)
        self.refresh_tables()

    def handle_buy_fill(self, event: BuyFillEvent) -> None:
        row = self._find_list1_row(event.code)
        if row is None:
            return

        row["매수주문번호"] = event.order_no
        row["매수체결수량"] = event.cumulative_filled_qty
        row["매수미체결수량"] = event.unfilled_qty

        order_qty = to_int(row.get("매수 수량"))
        if event.cumulative_filled_qty >= order_qty and event.unfilled_qty == 0:
            row["매수주문상태"] = BUY_STATUS_FILLED
            self._move_to_list2_and_send_sells(row, event.cumulative_filled_qty)
            return

        if event.cumulative_filled_qty > 0:
            row["매수주문상태"] = BUY_STATUS_PARTIAL_WAIT
            self.runtime.first_fill_time_by_code.setdefault(event.code, event.event_time)

        self.refresh_tables()

    def handle_sell_fill(self, event: SellFillEvent) -> None:
        row = self._find_list2_row(event.code)
        if row is None:
            return

        self._ensure_sell_level_metadata(row)
        level = event.sell_level
        if level in {1, 2, 3}:
            row[f"매도가{level}체결수량"] = to_int(row.get(f"매도가{level}체결수량")) + event.fill_qty
        row["잔여 수량"] = max(0, event.remaining_holding_qty)
        row["매도 수량"] = to_int(row.get("매도 수량")) + event.fill_qty
        row["매도 금액"] = to_int(row.get("매도 금액")) + (event.fill_qty * event.fill_price)
        row["매도 횟수"] = to_int(row.get("매도 횟수")) + 1
        if level in {1, 2, 3}:
            self._sync_sell_level_qty_after_fill(row, level)

        if row["잔여 수량"] <= 0:
            self._move_to_list3(row)
            return

        if level in {1, 2, 3}:
            self._apply_profit_stop_loss_after_fill(row, level, event.fill_price)
        self._request_profit_market_liquidation_if_needed(row)
        self.refresh_tables()

    def handle_sell_cancel_confirmed(self, event: SellCancelConfirmedEvent) -> None:
        row = self._find_list2_row(event.code)
        if row is None:
            return
        self._ensure_sell_level_metadata(row)
        row["잔여 수량"] = max(0, event.remaining_holding_qty)
        status = row.get("매도전략상태")
        if status == SELL_STATUS_RELOCATION_CANCEL_REQUESTED:
            self._complete_full_sell_relocation(row)
        elif status == SELL_STATUS_RELOCATION_LEVEL1_CANCEL_REQUESTED:
            self._complete_level1_sell_relocation(row)
        elif status == SELL_STATUS_CANCEL_REQUESTED:
            qty = to_int(row.get("잔여 수량"))
            if qty > 0:
                order_no = self.kiwoom.send_market_sell_order(row, qty)
                row["시장가주문번호"] = order_no or ""
                row["매도전략상태"] = SELL_STATUS_MARKET_REQUESTED
                self.sell_log(f"{self._name(row)}: 익절 추적 손절 시장가 매도 요청")
        self.refresh_tables()

    def handle_market_sell_accepted(self, event: MarketSellAcceptedEvent) -> None:
        row = self._find_list2_row(event.code)
        if row is None:
            return
        row["시장가주문번호"] = event.order_no
        row["매도전략상태"] = SELL_STATUS_MARKET_REQUESTED
        self.refresh_tables()

    def handle_buy_cancel_confirmed(self, event: BuyCancelConfirmedEvent) -> None:
        row = self._find_list1_row(event.code)
        if row is None:
            return

        row["매수주문번호"] = event.order_no
        row["매수미체결수량"] = event.unfilled_qty
        if event.unfilled_qty != 0:
            self.refresh_tables()
            return

        row["매수주문상태"] = BUY_STATUS_CANCEL_DONE
        filled_qty = to_int(row.get("매수체결수량"))
        if filled_qty > 0:
            self._move_to_list2_and_send_sells(row, filled_qty)
            return

        self._remove_list1_row(row)
        self.refresh_tables()

    def process_time_events(self, now: datetime) -> None:
        for row in list(self.list1):
            code = str(row.get("종목코드", ""))
            status = row.get("매수주문상태", BUY_STATUS_NOT_ORDERED)

            if status == BUY_STATUS_PARTIAL_WAIT:
                first_fill_time = self.runtime.first_fill_time_by_code.get(code)
                if first_fill_time and now - first_fill_time >= timedelta(seconds=PARTIAL_FILL_WAIT_SECONDS):
                    self._request_buy_cancel(row, "부분 체결 잔량 취소")
                continue

            if status in {BUY_STATUS_ACCEPTED, BUY_STATUS_REQUESTED} and to_int(row.get("매수체결수량")) == 0:
                if now.time() >= time(9, 40, 0):
                    self.buy_log(f"{self._name(row)}: 매수 지연 취소")
                    self._request_buy_cancel(row, "매수 지연 취소")

        self.refresh_tables()

    def _buy_wait_cancel_reason(self, row: Dict[str, Any], now: datetime) -> str:
        current_price = to_int(row.get("현재가"))
        open_price = to_int(row.get("시가"))
        current_volume = to_int(row.get("실시간 거래량"))
        previous_volume = to_int(row.get("전일 거래량"))
        if current_price <= 0 or open_price <= 0 or previous_volume <= 0:
            return ""
        if current_price >= open_price:
            return ""

        now_time = now.time()
        if time(9, 0, 0) <= now_time < time(9, 12, 0):
            if current_volume >= previous_volume * 0.3:
                return "음봉 거래량 취소"
        return ""

    def _request_buy_cancel(self, row: Dict[str, Any], reason: str) -> None:
        if row.get("매수주문상태") == BUY_STATUS_CANCEL_REQUESTED:
            return

        row["매수주문상태"] = BUY_STATUS_CANCEL_REQUESTED
        row["감시상태"] = WATCH_STATUS_OFF
        row["취소사유"] = reason
        self.kiwoom.stop_realtime_watch(row)
        if not self.kiwoom.cancel_buy_order(row):
            self.buy_log(f"{self._name(row)}: 매수 취소 요청 실패 - {reason}")

    def _move_to_list2_and_send_sells(self, row: Dict[str, Any], holding_qty: int) -> None:
        list2_row = self._make_list2_row(row, holding_qty)
        for level, (price, qty) in enumerate(build_sell_slices(
            holding_qty,
            [row.get("매도가1"), row.get("매도가2"), row.get("매도가3")],
        ), start=1):
            self._send_sell_level_order(list2_row, level, price, qty)

        self._sync_sell_order_numbers(list2_row)
        self.list2.append(list2_row)
        self._remove_list1_row(row)
        self.sell_log(f"{self._name(row)}: 매도 주문 등록")
        self.refresh_tables()

    def _apply_profit_stop_loss_after_fill(self, row: Dict[str, Any], level: int, fill_price: int) -> None:
        if level not in {1, 2, 3}:
            return

        if row.get("매도재배치여부"):
            if level == 1:
                row["매도전략상태"] = SELL_STRATEGY_RELOCATED_PROFIT
                stop_price = to_int(row.get("저가"))
            else:
                previous_price = to_int(row.get(f"매도가{level - 1}"))
                current_price = to_int(row.get(f"매도가{level}"))
                stop_price = self._profit_stop_price(previous_price, current_price)
            if stop_price <= 0:
                return
            row["손절가"] = stop_price
            self.sell_log(f"{self._name(row)}: 손절가 갱신 {row['손절가']:,}원")
            return

        if level == 1:
            previous_price = to_int(row.get("매수가"))
            current_price = to_int(row.get("매도가1"))
            row["매도전략상태"] = SELL_STRATEGY_PROFIT
        else:
            previous_price = to_int(row.get(f"매도가{level - 1}"))
            current_price = to_int(row.get(f"매도가{level}"))

        stop_price = self._profit_stop_price(previous_price, current_price)
        if stop_price <= 0:
            return
        row["손절가"] = stop_price
        self.sell_log(f"{self._name(row)}: 손절가 갱신 {row['손절가']:,}원")

    def _profit_stop_price(self, previous_price: int, current_price: int) -> int:
        if previous_price <= 0 or current_price <= previous_price:
            return 0
        return ceil_to_krx_tick(((current_price - previous_price) * 0.1) + previous_price)

    def _request_sell_relocation_if_needed(self, row: Dict[str, Any]) -> None:
        if row.get("감시 상태", WATCH_STATUS_ON) != WATCH_STATUS_ON:
            return
        if row.get("매도전략상태") in {
            SELL_STATUS_RELOCATION_CANCEL_REQUESTED,
            SELL_STATUS_RELOCATION_LEVEL1_CANCEL_REQUESTED,
            SELL_STATUS_CANCEL_REQUESTED,
            SELL_STATUS_MARKET_REQUESTED,
        }:
            self._remember_lower_pending_low(row)
            return

        buy_price = to_int(row.get("매수가"))
        current_price = to_int(row.get("현재가"))
        low_price = to_int(row.get("저가"))
        if buy_price <= 0 or current_price <= 0 or low_price <= 0:
            return
        if current_price >= buy_price * 0.96:
            return

        if not row.get("매도재배치여부"):
            self._request_full_sell_relocation(row)
            return

        last_low = to_int(row.get("매도재배치저가"))
        if last_low <= 0 or low_price < last_low:
            self._request_level1_sell_relocation(row)

    def _request_full_sell_relocation(self, row: Dict[str, Any]) -> None:
        row["재배치기준매도가1"] = to_int(row.get("매도가1"))
        row["재배치기준매도가2"] = to_int(row.get("매도가2"))
        row["재배치기준매도가수"] = self._sell_level_count(row)
        row["매도재배치대기저가"] = to_int(row.get("저가"))
        row["매도전략상태"] = SELL_STATUS_RELOCATION_CANCEL_REQUESTED
        if self._has_active_sell_order(row):
            if not self.kiwoom.cancel_sell_orders(row):
                self.sell_log(f"{self._name(row)}: 매도 재배치 지정가 취소 요청 실패")
                row["매도전략상태"] = SELL_STRATEGY_NORMAL
                return
            self.sell_log(f"{self._name(row)}: 매도 재배치 지정가 전체 취소 요청")
            return
        self._complete_full_sell_relocation(row)

    def _request_level1_sell_relocation(self, row: Dict[str, Any]) -> None:
        if self._remaining_sell_qty_by_level(row, 1) <= 0:
            return
        row["매도재배치대기저가"] = to_int(row.get("저가"))
        row["매도전략상태"] = SELL_STATUS_RELOCATION_LEVEL1_CANCEL_REQUESTED
        order_no = str(row.get("매도가1주문번호", ""))
        if order_no:
            if not self.kiwoom.cancel_sell_order(row, order_no):
                self.sell_log(f"{self._name(row)}: 매도가1 재배치 취소 요청 실패")
                row["매도전략상태"] = SELL_STRATEGY_RELOCATED
                return
            self.sell_log(f"{self._name(row)}: 매도가1 재배치 취소 요청")
            return
        self._complete_level1_sell_relocation(row)

    def _complete_full_sell_relocation(self, row: Dict[str, Any]) -> None:
        low_price = to_int(row.get("매도재배치대기저가", row.get("저가")))
        previous_sell1 = to_int(row.get("재배치기준매도가1", row.get("매도가1")))
        previous_sell2 = to_int(row.get("재배치기준매도가2", row.get("매도가2")))
        sell_level_count = to_int(row.get("재배치기준매도가수")) or self._sell_level_count(row)
        if low_price <= 0 or previous_sell1 <= 0:
            row["매도전략상태"] = SELL_STRATEGY_NORMAL
            return

        new_sell1 = self._relocated_sell1_price(low_price, previous_sell1)
        sell_prices = [new_sell1]
        if sell_level_count >= 2:
            sell_prices.append(previous_sell1)
        if sell_level_count >= 3 and previous_sell2 > 0:
            sell_prices.append(previous_sell2)

        for level in (1, 2, 3):
            if level <= len(sell_prices):
                price = sell_prices[level - 1]
                qty = self._remaining_sell_qty_by_level(row, level)
                if sell_level_count == 1:
                    qty = to_int(row.get("잔여 수량"))
                self._send_sell_level_order(row, level, price, qty)
            else:
                self._clear_sell_level_order(row, level)

        row["매도재배치여부"] = True
        row["매도재배치저가"] = low_price
        row["매도전략상태"] = SELL_STRATEGY_RELOCATED
        self._sync_sell_order_numbers(row)
        self.sell_log(f"{self._name(row)}: 매도 재배치 완료")

    def _complete_level1_sell_relocation(self, row: Dict[str, Any]) -> None:
        low_price = to_int(row.get("매도재배치대기저가", row.get("저가")))
        sell2 = to_int(row.get("매도가2"))
        if low_price <= 0:
            row["매도전략상태"] = SELL_STRATEGY_RELOCATED
            return

        new_sell1 = self._relocated_sell1_price(low_price, sell2)
        qty = self._remaining_sell_qty_by_level(row, 1)
        self._send_sell_level_order(row, 1, new_sell1, qty)
        row["매도재배치저가"] = low_price
        if to_int(row.get("매도가1체결수량")) > 0:
            row["매도전략상태"] = SELL_STRATEGY_RELOCATED_PROFIT
            row["손절가"] = low_price
        else:
            row["매도전략상태"] = SELL_STRATEGY_RELOCATED
        self._sync_sell_order_numbers(row)
        self.sell_log(f"{self._name(row)}: 매도가1 재배치 완료")

    def _relocated_sell1_price(self, low_price: int, sell2_price: int) -> int:
        sell1 = ceil_to_krx_tick(low_price * 1.03)
        if sell2_price > 0 and sell1 > sell2_price:
            below_sell2 = previous_krx_tick(sell2_price)
            if below_sell2 > 0:
                sell1 = below_sell2
        return sell1

    def _remember_lower_pending_low(self, row: Dict[str, Any]) -> None:
        status = row.get("매도전략상태")
        if status not in {SELL_STATUS_RELOCATION_CANCEL_REQUESTED, SELL_STATUS_RELOCATION_LEVEL1_CANCEL_REQUESTED}:
            return
        low_price = to_int(row.get("저가"))
        pending_low = to_int(row.get("매도재배치대기저가"))
        if low_price > 0 and (pending_low <= 0 or low_price < pending_low):
            row["매도재배치대기저가"] = low_price

    def _update_relocated_stop_loss_from_low(self, row: Dict[str, Any]) -> None:
        if row.get("매도전략상태") != SELL_STRATEGY_RELOCATED_PROFIT:
            return
        if to_int(row.get("매도가1체결수량")) <= 0:
            return
        if to_int(row.get("매도가2체결수량")) > 0:
            return
        low_price = to_int(row.get("저가"))
        if low_price > 0 and low_price != to_int(row.get("손절가")):
            row["손절가"] = low_price
            self.sell_log(f"{self._name(row)}: 재배치 손절가 저가 갱신 {low_price:,}원")

    def _send_sell_level_order(self, row: Dict[str, Any], level: int, price: int, qty: int) -> None:
        if level not in {1, 2, 3}:
            return
        row[f"매도가{level}"] = price
        if qty <= 0 or price <= 0:
            row[f"매도가{level}주문번호"] = ""
            row[f"매도가{level}주문수량"] = 0
            return
        if to_int(row.get(f"매도가{level}주문수량")) <= 0:
            row[f"매도가{level}주문수량"] = qty
        order_no = self.kiwoom.send_sell_order(row, price, qty)
        row[f"매도가{level}주문번호"] = order_no or ""

    def _clear_sell_level_order(self, row: Dict[str, Any], level: int) -> None:
        if level not in {1, 2, 3}:
            return
        row[f"매도가{level}"] = None
        row[f"매도가{level}주문번호"] = ""
        row[f"매도가{level}주문수량"] = 0

    def _sell_level_count(self, row: Dict[str, Any]) -> int:
        return sum(
            1
            for level in (1, 2, 3)
            if to_int(row.get(f"매도가{level}")) > 0 or to_int(row.get(f"매도가{level}주문수량")) > 0
        )

    def _ensure_sell_level_metadata(self, row: Dict[str, Any]) -> None:
        if not any(to_int(row.get(f"매도가{level}주문수량")) > 0 for level in (1, 2, 3)):
            for level, (_price, qty) in enumerate(build_sell_slices(
                to_int(row.get("잔여 수량")),
                [row.get("매도가1"), row.get("매도가2"), row.get("매도가3")],
            ), start=1):
                row[f"매도가{level}주문수량"] = qty

        if not any(str(row.get(f"매도가{level}주문번호", "")) for level in (1, 2, 3)):
            order_numbers = [
                order_no.strip()
                for order_no in str(row.get("매도주문번호들", "")).split(",")
                if order_no.strip()
            ]
            for level, order_no in enumerate(order_numbers[:3], start=1):
                row[f"매도가{level}주문번호"] = order_no

    def _sync_sell_order_numbers(self, row: Dict[str, Any]) -> None:
        row["매도주문번호들"] = ",".join(
            order_no
            for order_no in [
                str(row.get("매도가1주문번호", "")),
                str(row.get("매도가2주문번호", "")),
                str(row.get("매도가3주문번호", "")),
            ]
            if order_no
        )

    def _sync_sell_level_qty_after_fill(self, row: Dict[str, Any], level: int) -> None:
        ordered_qty = to_int(row.get(f"매도가{level}주문수량"))
        filled_qty = to_int(row.get(f"매도가{level}체결수량"))
        if ordered_qty > 0 and filled_qty >= ordered_qty:
            row[f"매도가{level}주문번호"] = ""
        self._sync_sell_order_numbers(row)

    def _remaining_sell_qty_by_level(self, row: Dict[str, Any], level: int) -> int:
        ordered_qty = to_int(row.get(f"매도가{level}주문수량"))
        filled_qty = to_int(row.get(f"매도가{level}체결수량"))
        return max(0, ordered_qty - filled_qty)

    def _has_active_sell_order(self, row: Dict[str, Any]) -> bool:
        for level in (1, 2, 3):
            if str(row.get(f"매도가{level}주문번호", "")) and self._remaining_sell_qty_by_level(row, level) > 0:
                return True
        return bool(str(row.get("매도주문번호들", "")))

    def _request_profit_market_liquidation_if_needed(self, row: Dict[str, Any]) -> None:
        current_price = to_int(row.get("현재가"))
        stop_price = to_int(row.get("손절가"))
        holding_qty = to_int(row.get("잔여 수량"))
        if current_price <= 0 or stop_price <= 0 or holding_qty <= 0:
            return
        if current_price > stop_price:
            return
        if row.get("매도전략상태") in {
            SELL_STATUS_CANCEL_REQUESTED,
            SELL_STATUS_MARKET_REQUESTED,
            SELL_STATUS_RELOCATION_CANCEL_REQUESTED,
            SELL_STATUS_RELOCATION_LEVEL1_CANCEL_REQUESTED,
        }:
            return

        row["매도전략상태"] = SELL_STATUS_CANCEL_REQUESTED
        row["청산사유"] = "익절 추적 손절"
        if not self.kiwoom.cancel_sell_orders(row):
            self.sell_log(f"{self._name(row)}: 잔여 지정가 매도 취소 요청 실패")
            return
        self.sell_log(f"{self._name(row)}: 잔여 지정가 취소 요청 - 익절 추적 손절")

    def _move_to_list3(self, row: Dict[str, Any]) -> None:
        summary = self._make_list3_row(row)
        self.list3.append(summary)
        self._remove_list2_row(row)
        self.sell_log(f"{self._name(row)}: 매매 결과 요약 이동")
        self.refresh_tables()

    def _make_list3_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        summary = make_trade_summary_row(row)
        summary["매수가"] = row.get("매수가")
        return summary

    def _make_list2_row(self, row: Dict[str, Any], holding_qty: int) -> Dict[str, Any]:
        return {
            "종목코드": row.get("종목코드", ""),
            "종목명": row.get("종목명", ""),
            "매수가": row.get("매수가"),
            "매수 수량": holding_qty,
            "잔여 수량": holding_qty,
            "매도 횟수": 0,
            "현재가": row.get("현재가"),
            "저가": row.get("저가"),
            "매도가1": row.get("매도가1"),
            "매도가2": row.get("매도가2"),
            "매도가3": row.get("매도가3"),
            "손절가": row.get("손절가"),
            "매도 금액": 0,
            "매수주문번호": row.get("매수주문번호", ""),
            "매도주문번호들": "",
            "매도전략상태": SELL_STRATEGY_NORMAL,
            "매도가1체결수량": 0,
            "매도가2체결수량": 0,
            "매도가3체결수량": 0,
            "매도 수량": 0,
            "시장가주문번호": "",
            "청산사유": "",
            "감시 상태": WATCH_STATUS_ON,
            "매도가1주문번호": "",
            "매도가2주문번호": "",
            "매도가3주문번호": "",
            "매도가1주문수량": 0,
            "매도가2주문수량": 0,
            "매도가3주문수량": 0,
            "매도재배치여부": False,
            "매도재배치저가": 0,
            "매도재배치대기저가": 0,
            "재배치기준매도가1": 0,
            "재배치기준매도가2": 0,
            "재배치기준매도가수": 0,
        }

    def _handle_buy_order_request_failed(self, row: Dict[str, Any], reason: str) -> None:
        row["매수주문상태"] = BUY_STATUS_FAILED
        row["감시상태"] = WATCH_STATUS_OFF
        row["취소사유"] = reason
        self.buy_log(f"{self._name(row)}: {reason}")
        self.refresh_tables()

    def _reset_order_after_user_cancel(self, row: Dict[str, Any]) -> None:
        row["매수주문번호"] = ""
        row["매수주문상태"] = BUY_STATUS_NOT_ORDERED
        row["매수체결수량"] = 0
        row["매수미체결수량"] = to_int(row.get("매수 수량"))

    def _row_by_index(self, row_idx: int) -> Optional[Dict[str, Any]]:
        if 0 <= row_idx < len(self.list1):
            return self.list1[row_idx]
        return None

    def _find_list1_row(self, code: str) -> Optional[Dict[str, Any]]:
        for row in self.list1:
            if str(row.get("종목코드", "")) == str(code):
                return row
        return None

    def _find_list2_row(self, code: str) -> Optional[Dict[str, Any]]:
        for row in self.list2:
            if str(row.get("종목코드", "")) == str(code):
                return row
        return None

    def _remove_list1_row(self, target: Dict[str, Any]) -> None:
        code = str(target.get("종목코드", ""))
        self.list1[:] = [
            row
            for row in self.list1
            if str(row.get("종목코드", "")) != code
        ]
        self.runtime.first_fill_time_by_code.pop(code, None)
        self.runtime.retry_count_by_code.pop(code, None)

    def _remove_list2_row(self, target: Dict[str, Any]) -> None:
        code = str(target.get("종목코드", ""))
        self.list2[:] = [
            row
            for row in self.list2
            if str(row.get("종목코드", "")) != code
        ]

    def _name(self, row: Dict[str, Any]) -> str:
        return str(row.get("종목명") or row.get("종목코드") or "")
