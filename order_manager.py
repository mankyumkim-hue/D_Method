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
SELL_STATUS_STOP_LIMIT_SWITCH = "손절지정가전환"
SELL_STATUS_STOP_LIMIT_READY = "손절지정가대기"
SELL_STATUS_STOP_LIMIT_ORDERED = "손절지정가주문중"
SELL_STATUS_STOP_LIMIT_RESTORE = "손절지정가복구중"
SELL_STATUS_STOP_LIMIT_TARGET_MARKET = "손절지정가목표시장가전환"
SELL_STATUS_STOP_LIMIT_CLOSE_WAIT = "손절지정가청산대기"
SELL_STATUS_MARKET_REQUESTED = "시장가청산요청"
SELL_STATUS_DONE = "청산완료"

WATCH_STATUS_ON = "감시 중"
WATCH_STATUS_OFF = "감시 중지"

BUY_RETRY_LIMIT = 1
PARTIAL_FILL_WAIT_SECONDS = 6
SELL_CANCEL_TIMEOUT_SECONDS = 30


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

    def cancel_sell_order_qty(self, row: Dict[str, Any], order_no: str, qty: int) -> bool:
        """row의 특정 지정가 매도 주문 중 qty만큼 부분 취소 요청한다."""

    def send_market_sell_order(self, row: Dict[str, Any], qty: int) -> Optional[str]:
        """row의 잔여 보유수량을 시장가로 매도 요청하고, 접수된 주문번호를 반환한다."""

    def start_realtime_watch(self, row: Dict[str, Any]) -> None:
        """현재가, 저가, 누적 거래량 실시간 감시를 시작한다."""

    def stop_realtime_watch(self, row: Dict[str, Any]) -> None:
        """실시간 감시를 중단한다."""

    def request_account_balance_sync(self, reason: str = "") -> bool:
        """HTS 계좌 잔고 조회를 요청한다."""

    def request_trade_fill_sync(self, row: Dict[str, Any], reason: str = "") -> bool:
        """HTS 체결내역 조회를 요청한다."""


@dataclass
class BuyFillEvent:
    code: str
    order_no: str
    fill_qty: int
    cumulative_filled_qty: int
    unfilled_qty: int
    fill_price: int
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
class AccountBalanceSyncEvent:
    positions: Dict[str, int]
    event_time: datetime


@dataclass
class TradeFillSyncEvent:
    code: str
    buy_qty: int
    buy_amount: int
    sell_qty: int
    sell_amount: int
    average_sell_price: int
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
    open_price: int = 0


@dataclass
class OrderRuntimeState:
    retry_count_by_code: Dict[str, int] = field(default_factory=dict)
    first_fill_time_by_code: Dict[str, datetime] = field(default_factory=dict)
    sell_cancel_requested_at_by_code: Dict[str, datetime] = field(default_factory=dict)


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
        previous_low = to_int(row.get("직전저가")) or to_int(row.get("저가"))
        row["현재가"] = quote.current_price
        if self._should_request_noon_low_break_partial_market_sell(row, quote, previous_low):
            self._request_partial_market_sell(row, "12시 저가 갱신 부분청산", "12시저가부분청산완료")
        row["저가"] = quote.low_price
        if quote.low_price > 0:
            row["직전저가"] = quote.low_price
        self._request_stop_limit_transition_if_needed(row)
        self._request_stop_limit_target_market_if_needed(row)
        self._request_stop_limit_restore_if_needed(row)
        self._request_sell_relocation_if_needed(row)
        self._update_relocated_stop_loss_from_low(row)
        self.refresh_tables()

    def handle_buy_fill(self, event: BuyFillEvent) -> None:
        row = self._find_list1_row(event.code)
        if row is None:
            return

        row["매수주문번호"] = event.order_no
        row["매수체결수량"] = event.cumulative_filled_qty
        row["매수미체결수량"] = event.unfilled_qty
        if event.fill_qty > 0 and event.fill_price > 0:
            row["매수체결금액"] = to_int(row.get("매수체결금액")) + (event.fill_qty * event.fill_price)
            filled_qty = max(1, event.cumulative_filled_qty)
            row["실제 매수가"] = round(to_int(row.get("매수체결금액")) / filled_qty)

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
        fill_qty = min(event.fill_qty, max(0, to_int(row.get("잔여 수량"))))
        if fill_qty <= 0:
            return
        if level in {1, 2, 3}:
            row[f"매도가{level}체결수량"] = to_int(row.get(f"매도가{level}체결수량")) + fill_qty
        row["잔여 수량"] = max(0, event.remaining_holding_qty)
        row["매도 수량"] = to_int(row.get("매도 수량")) + fill_qty
        row["매도 금액"] = to_int(row.get("매도 금액")) + (fill_qty * event.fill_price)
        row["매도 횟수"] = to_int(row.get("매도 횟수")) + 1
        if level in {1, 2, 3}:
            self._sync_sell_level_qty_after_fill(row, level)

        if row["잔여 수량"] <= 0:
            self._move_to_list3(row)
            return

        if level in {1, 2, 3}:
            self._apply_profit_stop_loss_after_fill(row, level, event.fill_price)
        if level == 0 and row.get("매도전략상태") in {
            SELL_STATUS_STOP_LIMIT_ORDERED,
            SELL_STATUS_STOP_LIMIT_RESTORE,
            SELL_STATUS_STOP_LIMIT_TARGET_MARKET,
            SELL_STATUS_STOP_LIMIT_CLOSE_WAIT,
        }:
            row["손절지정가체결수량"] = to_int(row.get("손절지정가체결수량")) + fill_qty
        if level == 0 and row.pop("손절지정가목표시장가주문중", False) and row["잔여 수량"] > 0:
            row["매도전략상태"] = SELL_STRATEGY_RELOCATED if row.get("매도재배치여부") else SELL_STRATEGY_NORMAL
        self._request_stop_limit_transition_if_needed(row)
        self._request_stop_limit_restore_if_needed(row)
        self.refresh_tables()

    def handle_sell_cancel_confirmed(self, event: SellCancelConfirmedEvent) -> None:
        row = self._find_list2_row(event.code)
        if row is None:
            return
        self._ensure_sell_level_metadata(row)
        row["잔여 수량"] = max(0, event.remaining_holding_qty)
        status = row.get("매도전략상태")
        if status == SELL_STATUS_RELOCATION_CANCEL_REQUESTED:
            self.runtime.sell_cancel_requested_at_by_code.pop(event.code, None)
            self._complete_full_sell_relocation(row)
        elif status == SELL_STATUS_RELOCATION_LEVEL1_CANCEL_REQUESTED:
            self.runtime.sell_cancel_requested_at_by_code.pop(event.code, None)
            self._complete_level1_sell_relocation(row)
        elif status == SELL_STATUS_STOP_LIMIT_SWITCH:
            self.runtime.sell_cancel_requested_at_by_code.pop(event.code, None)
            self._complete_stop_limit_switch(row)
        elif status == SELL_STATUS_STOP_LIMIT_READY:
            self._request_stop_limit_transition_if_needed(row)
            self._request_stop_limit_restore_if_needed(row)
        elif status == SELL_STATUS_STOP_LIMIT_RESTORE:
            self.runtime.sell_cancel_requested_at_by_code.pop(event.code, None)
            self._complete_stop_limit_restore(row)
        elif status == SELL_STATUS_STOP_LIMIT_TARGET_MARKET:
            self.runtime.sell_cancel_requested_at_by_code.pop(event.code, None)
            self._complete_stop_limit_target_market(row)
        elif status == SELL_STATUS_STOP_LIMIT_CLOSE_WAIT:
            self.runtime.sell_cancel_requested_at_by_code.pop(event.code, None)
            row["매도전략상태"] = SELL_STRATEGY_RELOCATED if row.get("매도재배치여부") else SELL_STRATEGY_NORMAL
            row["손절지정가주문번호"] = ""
            row["손절지정가주문수량"] = 0
        elif status == SELL_STATUS_CANCEL_REQUESTED:
            self.runtime.sell_cancel_requested_at_by_code.pop(event.code, None)
            qty = to_int(row.get("잔여 수량"))
            if qty > 0:
                order_no = self.kiwoom.send_market_sell_order(row, qty)
                row["시장가주문번호"] = order_no or ""
                row["매도전략상태"] = SELL_STATUS_MARKET_REQUESTED
                reason = str(row.get("청산사유") or "익절 추적 손절")
                self.sell_log(f"{self._name(row)}: {reason} 시장가 매도 요청")
        self.refresh_tables()

    def handle_market_sell_accepted(self, event: MarketSellAcceptedEvent) -> None:
        row = self._find_list2_row(event.code)
        if row is None:
            return
        row["시장가주문번호"] = event.order_no
        row["매도전략상태"] = SELL_STATUS_MARKET_REQUESTED
        self.refresh_tables()

    def handle_account_balance_sync(self, event: AccountBalanceSyncEvent) -> None:
        changed = False
        for row in list(self.list2):
            code = str(row.get("종목코드", ""))
            if not code:
                continue
            hts_qty = to_int(event.positions.get(code, 0))
            internal_qty = to_int(row.get("잔여 수량"))
            if hts_qty == internal_qty:
                row["HTS 잔고수량"] = hts_qty
                continue

            row["HTS 잔고수량"] = hts_qty
            row["잔여 수량"] = max(0, hts_qty)
            self.sell_log(
                f"{self._name(row)}: HTS 잔고 동기화 잔여수량 {internal_qty}주 -> {hts_qty}주"
            )
            changed = True
            if hts_qty <= 0:
                row["청산사유"] = row.get("청산사유") or "HTS 잔고 동기화"
                if self._should_sync_trade_fills_before_summary(row):
                    if self.kiwoom.request_trade_fill_sync(row, "HTS 잔고 0 보정"):
                        row["매매요약대기"] = True
                        continue
                self._move_to_list3(row)

        if changed:
            self.refresh_tables()

    def handle_trade_fill_sync(self, event: TradeFillSyncEvent) -> None:
        row = self._find_list2_row(event.code)
        if row is None:
            return

        current_buy_qty = to_int(row.get("매수 수량"))
        current_buy_amount = to_int(row.get("매수체결금액")) or to_int(row.get("매수 금액"))
        if event.buy_qty > 0:
            row["매수 수량"] = event.buy_qty
            if event.buy_amount > 0:
                row["매수체결금액"] = event.buy_amount
                row["매수 금액"] = event.buy_amount
                row["평균 매수 금액"] = round_half_up(event.buy_amount / event.buy_qty)
                row["매수가"] = row["평균 매수 금액"]
            else:
                fallback_buy_price = to_int(row.get("평균 매수 금액")) or to_int(row.get("매수가"))
                fallback_buy_amount = fallback_buy_price * event.buy_qty
                if fallback_buy_amount > 0:
                    row["매수체결금액"] = fallback_buy_amount
                    row["매수 금액"] = fallback_buy_amount
                    row["평균 매수 금액"] = fallback_buy_price
            if event.buy_qty != current_buy_qty:
                self.sell_log(
                    f"{self._name(row)}: HTS 체결내역 보정 매수수량 {current_buy_qty}주 -> {event.buy_qty}주"
                )
        elif current_buy_amount > 0 and current_buy_qty > 0:
            row["평균 매수 금액"] = round_half_up(current_buy_amount / current_buy_qty)

        current_sell_qty = to_int(row.get("매도 수량"))
        current_sell_amount = to_int(row.get("매도 금액"))
        synced_qty = event.sell_qty
        synced_amount = event.sell_amount
        if synced_qty > current_sell_qty:
            row["매도 수량"] = synced_qty
            if synced_amount > 0:
                row["매도 금액"] = max(current_sell_amount, synced_amount)
            else:
                fallback_sell_price = to_int(row.get("평균 매도 금액"))
                if fallback_sell_price <= 0 and current_sell_qty > 0 and current_sell_amount > 0:
                    fallback_sell_price = round_half_up(current_sell_amount / current_sell_qty)
                row["매도 금액"] = fallback_sell_price * synced_qty if fallback_sell_price > 0 else current_sell_amount
            if to_int(row.get("매도 수량")) > 0 and to_int(row.get("매도 금액")) > 0:
                row["평균 매도 금액"] = round_half_up(
                    to_int(row.get("매도 금액")) / to_int(row.get("매도 수량"))
                )
            row["매도 횟수"] = max(to_int(row.get("매도 횟수")), 1)
            self.sell_log(
                f"{self._name(row)}: HTS 체결내역 보정 매도수량 {current_sell_qty}주 -> {synced_qty}주"
            )
        elif synced_amount > current_sell_amount:
            row["매도 금액"] = synced_amount
            if current_sell_qty > 0:
                row["평균 매도 금액"] = round_half_up(synced_amount / current_sell_qty)
        elif current_sell_amount > 0 and current_sell_qty > 0:
            row["평균 매도 금액"] = round_half_up(current_sell_amount / current_sell_qty)

        if (
            event.average_sell_price > 0
            and event.sell_qty > 0
            and event.sell_qty == to_int(row.get("매도 수량"))
        ):
            current_average_sell_price = to_int(row.get("평균 매도 금액"))
            row["평균 매도 금액"] = event.average_sell_price
            if current_average_sell_price != event.average_sell_price:
                self.sell_log(
                    f"{self._name(row)}: HTS 평균 매도가 보정 "
                    f"{current_average_sell_price:,}원 -> {event.average_sell_price:,}원"
                )
        elif event.average_sell_price > 0:
            self.sell_log(
                f"{self._name(row)}: HTS 평균 매도가 보정 건너뜀 "
                f"(조회 {event.sell_qty}주, 전체 {to_int(row.get('매도 수량'))}주)"
            )

        row["HTS 체결내역보정완료"] = True
        row.pop("매매요약대기", None)
        if to_int(row.get("잔여 수량")) <= 0:
            self._move_to_list3(row)
        else:
            self.refresh_tables()

    def retry_pending_sell_action(self, code: str) -> None:
        row = self._find_list2_row(code)
        if row is None:
            return
        status = row.get("매도전략상태")
        if status == SELL_STATUS_RELOCATION_CANCEL_REQUESTED:
            self._request_full_sell_relocation(row)
        elif status == SELL_STATUS_RELOCATION_LEVEL1_CANCEL_REQUESTED:
            self._request_level1_sell_relocation(row)
        elif status == SELL_STATUS_CANCEL_REQUESTED:
            row["매도전략상태"] = row.pop("취소전매도전략상태", SELL_STRATEGY_NORMAL)
            self._request_stop_limit_transition_if_needed(row)
        elif status == SELL_STATUS_STOP_LIMIT_SWITCH:
            self._request_stop_limit_transition_if_needed(row)
        elif status == SELL_STATUS_STOP_LIMIT_READY:
            self._request_stop_limit_transition_if_needed(row)
            self._request_stop_limit_restore_if_needed(row)
        elif status == SELL_STATUS_STOP_LIMIT_RESTORE:
            self._request_stop_limit_restore_if_needed(row)
        elif status == SELL_STATUS_STOP_LIMIT_TARGET_MARKET:
            self._request_stop_limit_target_market_if_needed(row)
        elif status in {
            SELL_STRATEGY_RELOCATED,
            SELL_STRATEGY_RELOCATED_PROFIT,
        }:
            self._request_sell_relocation_if_needed(row)

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

        for row in list(self.list2):
            if row.get("모의시간고정") and not row.get("_모의테스트"):
                continue
            self._ensure_sell_level_metadata(row)
            self._recover_stale_sell_cancel_request(row, now)
            self._request_1100_partial_market_sell_if_needed(row, now)
            self._request_stop_limit_close_wait_if_needed(row, now)
            self._request_closing_market_liquidation_if_needed(row, now)

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

    def _request_1100_partial_market_sell_if_needed(self, row: Dict[str, Any], now: datetime) -> None:
        if now.time() < time(11, 0, 0):
            return
        if to_int(row.get("매도가1체결수량")) > 0:
            return
        self._request_partial_market_sell(row, "11시 매도가1 미체결 부분청산", "11시부분청산완료")

    def _should_request_noon_low_break_partial_market_sell(
        self,
        row: Dict[str, Any],
        quote: RealtimeQuote,
        previous_low: int,
    ) -> bool:
        if quote.event_time.time() < time(12, 0, 0):
            return False
        if row.get("12시저가부분청산완료"):
            return False
        if to_int(row.get("매도가1체결수량")) > 0:
            return False
        return previous_low > 0 and quote.current_price > 0 and quote.current_price < previous_low

    def _request_partial_market_sell(self, row: Dict[str, Any], reason: str, done_key: str) -> None:
        if row.get(done_key):
            return
        if row.get("감시 상태", WATCH_STATUS_ON) != WATCH_STATUS_ON:
            return
        if self._has_pending_sell_strategy_action(row):
            return

        holding_qty = to_int(row.get("잔여 수량"))
        if holding_qty <= 0:
            return

        market_qty = 0
        for level in (1, 2, 3):
            remaining_qty = self._remaining_sell_qty_by_level(row, level)
            cancel_qty = min(remaining_qty, round_half_up(remaining_qty / 2))
            if cancel_qty <= 0:
                continue
            if not self._request_sell_level_partial_cancel(row, level, cancel_qty, reason):
                continue
            self._reduce_sell_level_order_qty(row, level, cancel_qty)
            market_qty += cancel_qty

        market_qty = min(market_qty, holding_qty)
        if market_qty <= 0:
            return

        order_no = self.kiwoom.send_market_sell_order(row, market_qty)
        self._append_market_order_no(row, order_no)
        row[done_key] = True
        row["청산사유"] = reason
        self.sell_log(f"{self._name(row)}: {reason} 시장가 매도 요청 {market_qty}주")

    def _request_sell_level_partial_cancel(
        self,
        row: Dict[str, Any],
        level: int,
        cancel_qty: int,
        reason: str,
    ) -> bool:
        order_no = str(row.get(f"매도가{level}주문번호", ""))
        remaining_qty = self._remaining_sell_qty_by_level(row, level)
        if not order_no:
            return True

        if cancel_qty >= remaining_qty:
            ok = self.kiwoom.cancel_sell_order(row, order_no)
        else:
            cancel_fn = getattr(self.kiwoom, "cancel_sell_order_qty", None)
            if not callable(cancel_fn):
                self.sell_log(f"{self._name(row)}: 매도가{level} 부분 취소 API 없음 - {reason}")
                return False
            ok = cancel_fn(row, order_no, cancel_qty)

        if not ok:
            self.sell_log(f"{self._name(row)}: 매도가{level} {cancel_qty}주 취소 요청 실패 - {reason}")
            return False
        self.sell_log(f"{self._name(row)}: 매도가{level} {cancel_qty}주 취소 요청 - {reason}")
        return True

    def _reduce_sell_level_order_qty(self, row: Dict[str, Any], level: int, cancel_qty: int) -> None:
        ordered_qty = to_int(row.get(f"매도가{level}주문수량"))
        filled_qty = to_int(row.get(f"매도가{level}체결수량"))
        next_ordered_qty = max(filled_qty, ordered_qty - cancel_qty)
        row[f"매도가{level}주문수량"] = next_ordered_qty
        if next_ordered_qty <= filled_qty:
            row[f"매도가{level}주문번호"] = ""
        self._sync_sell_order_numbers(row)

    def _request_closing_market_liquidation_if_needed(self, row: Dict[str, Any], now: datetime) -> None:
        if now.time() < time(15, 29, 50):
            return
        if row.get("장마감청산요청완료"):
            return
        if row.get("감시 상태", WATCH_STATUS_ON) != WATCH_STATUS_ON:
            return
        if self._has_pending_sell_strategy_action(row):
            return

        holding_qty = to_int(row.get("잔여 수량"))
        if holding_qty <= 0:
            return
        if not row.get("장마감잔고동기화완료"):
            if self.kiwoom.request_account_balance_sync("장마감 청산 전"):
                row["장마감잔고동기화완료"] = True
                return

        row["장마감청산요청완료"] = True
        row["청산사유"] = "장마감 청산"
        if self._has_active_sell_order(row):
            row["매도취소대기수"] = self._active_sell_order_count(row)
            if not self.kiwoom.cancel_sell_orders(row):
                row["장마감청산요청완료"] = False
                row["매도취소대기수"] = 0
                self.sell_log(f"{self._name(row)}: 장마감 잔여 지정가 취소 요청 실패")
                return
            row["매도전략상태"] = SELL_STATUS_CANCEL_REQUESTED
            self.sell_log(f"{self._name(row)}: 장마감 잔여 지정가 취소 요청")
            return

        order_no = self.kiwoom.send_market_sell_order(row, holding_qty)
        self._append_market_order_no(row, order_no)
        row["매도전략상태"] = SELL_STATUS_MARKET_REQUESTED
        self.sell_log(f"{self._name(row)}: 장마감 시장가 청산 요청 {holding_qty}주")

    def _has_pending_sell_strategy_action(self, row: Dict[str, Any]) -> bool:
        return row.get("매도전략상태") in {
            SELL_STATUS_RELOCATION_CANCEL_REQUESTED,
            SELL_STATUS_RELOCATION_LEVEL1_CANCEL_REQUESTED,
            SELL_STATUS_CANCEL_REQUESTED,
            SELL_STATUS_STOP_LIMIT_SWITCH,
            SELL_STATUS_STOP_LIMIT_READY,
            SELL_STATUS_STOP_LIMIT_ORDERED,
            SELL_STATUS_STOP_LIMIT_RESTORE,
            SELL_STATUS_STOP_LIMIT_TARGET_MARKET,
            SELL_STATUS_STOP_LIMIT_CLOSE_WAIT,
            SELL_STATUS_MARKET_REQUESTED,
        }

    def _should_sync_trade_fills_before_summary(self, row: Dict[str, Any]) -> bool:
        if row.get("매매요약대기"):
            return False
        if row.get("HTS 체결내역보정완료"):
            return False
        return bool(str(row.get("종목코드", "")).strip())

    def _append_market_order_no(self, row: Dict[str, Any], order_no: Optional[str]) -> None:
        if not order_no:
            return
        existing = [
            value.strip()
            for value in str(row.get("시장가주문번호", "")).split(",")
            if value.strip()
        ]
        existing.append(order_no)
        row["시장가주문번호"] = ",".join(existing)

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
            SELL_STATUS_STOP_LIMIT_SWITCH,
            SELL_STATUS_STOP_LIMIT_READY,
            SELL_STATUS_STOP_LIMIT_ORDERED,
            SELL_STATUS_STOP_LIMIT_RESTORE,
            SELL_STATUS_STOP_LIMIT_TARGET_MARKET,
            SELL_STATUS_STOP_LIMIT_CLOSE_WAIT,
            SELL_STATUS_MARKET_REQUESTED,
        }:
            self._remember_lower_pending_low(row)
            return

        buy_price = to_int(row.get("재배치 기준 매수가", row.get("매수가")))
        current_price = to_int(row.get("현재가"))
        low_price = to_int(row.get("저가"))
        if buy_price <= 0 or current_price <= 0 or low_price <= 0:
            return
        if current_price >= buy_price * 0.96:
            return

        if self._has_pending_sell_order(row):
            self._remember_pending_relocation_low(row)
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
        self._remember_sell_cancel_requested(row)
        if self._has_active_sell_order(row):
            row["매도취소대기수"] = self._active_sell_order_count(row)
            if not self.kiwoom.cancel_sell_orders(row):
                self.sell_log(f"{self._name(row)}: 매도 재배치 지정가 취소 요청 실패")
                row["매도전략상태"] = SELL_STRATEGY_NORMAL
                row["매도취소대기수"] = 0
                return
            self.sell_log(f"{self._name(row)}: 매도 재배치 지정가 전체 취소 요청")
            return
        self._complete_full_sell_relocation(row)

    def _request_level1_sell_relocation(self, row: Dict[str, Any]) -> None:
        if self._remaining_sell_qty_by_level(row, 1) <= 0:
            return
        row["매도재배치대기저가"] = to_int(row.get("저가"))
        row["매도전략상태"] = SELL_STATUS_RELOCATION_LEVEL1_CANCEL_REQUESTED
        row["매도취소대기수"] = 1
        self._remember_sell_cancel_requested(row)
        order_no = str(row.get("매도가1주문번호", ""))
        if order_no:
            if not self.kiwoom.cancel_sell_order(row, order_no):
                self.sell_log(f"{self._name(row)}: 매도가1 재배치 취소 요청 실패")
                row["매도전략상태"] = SELL_STRATEGY_RELOCATED
                row["매도취소대기수"] = 0
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
        row["매도취소대기수"] = 0
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
        row["매도취소대기수"] = 0
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
        self._remember_pending_relocation_low(row)

    def _remember_pending_relocation_low(self, row: Dict[str, Any]) -> None:
        low_price = to_int(row.get("저가"))
        pending_low = to_int(row.get("매도재배치대기저가"))
        if low_price > 0 and (pending_low <= 0 or low_price < pending_low):
            row["매도재배치대기저가"] = low_price

    def _remember_sell_cancel_requested(self, row: Dict[str, Any]) -> None:
        code = str(row.get("종목코드", ""))
        if code:
            self.runtime.sell_cancel_requested_at_by_code[code] = datetime.now()

    def _recover_stale_sell_cancel_request(self, row: Dict[str, Any], now: datetime) -> None:
        status = row.get("매도전략상태")
        if status not in {
            SELL_STATUS_RELOCATION_CANCEL_REQUESTED,
            SELL_STATUS_RELOCATION_LEVEL1_CANCEL_REQUESTED,
            SELL_STATUS_STOP_LIMIT_SWITCH,
            SELL_STATUS_STOP_LIMIT_RESTORE,
            SELL_STATUS_STOP_LIMIT_TARGET_MARKET,
            SELL_STATUS_STOP_LIMIT_CLOSE_WAIT,
        }:
            return
        code = str(row.get("종목코드", ""))
        requested_at = self.runtime.sell_cancel_requested_at_by_code.get(code)
        if requested_at is None:
            self.runtime.sell_cancel_requested_at_by_code[code] = now
            return
        if now - requested_at < timedelta(seconds=SELL_CANCEL_TIMEOUT_SECONDS):
            return

        self.runtime.sell_cancel_requested_at_by_code.pop(code, None)
        self.sell_log(f"{self._name(row)}: {status} {SELL_CANCEL_TIMEOUT_SECONDS}초 초과 - 재배치 완료 처리")
        if status == SELL_STATUS_RELOCATION_CANCEL_REQUESTED:
            self._complete_full_sell_relocation(row)
        elif status == SELL_STATUS_RELOCATION_LEVEL1_CANCEL_REQUESTED:
            self._complete_level1_sell_relocation(row)
        elif status == SELL_STATUS_STOP_LIMIT_SWITCH:
            self._complete_stop_limit_switch(row)
        elif status == SELL_STATUS_STOP_LIMIT_RESTORE:
            self._complete_stop_limit_restore(row)
        elif status == SELL_STATUS_STOP_LIMIT_TARGET_MARKET:
            self._complete_stop_limit_target_market(row)
        elif status == SELL_STATUS_STOP_LIMIT_CLOSE_WAIT:
            row["매도전략상태"] = SELL_STRATEGY_RELOCATED if row.get("매도재배치여부") else SELL_STRATEGY_NORMAL

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

    def _clear_all_sell_level_orders(self, row: Dict[str, Any], keep_prices: bool = False) -> None:
        for level in (1, 2, 3):
            if not keep_prices:
                row[f"매도가{level}"] = None
            row[f"매도가{level}주문번호"] = ""
            row[f"매도가{level}주문수량"] = 0
        self._sync_sell_order_numbers(row)

    def _remember_restore_sell_levels(self, row: Dict[str, Any]) -> None:
        if row.get("손절복구매도가저장완료"):
            return
        for level in (1, 2, 3):
            row[f"복구매도가{level}"] = row.get(f"매도가{level}")
            row[f"복구매도가{level}주문수량"] = self._remaining_sell_qty_by_level(row, level)
        row["손절복구매도가저장완료"] = True

    def _restore_sell_levels(self, row: Dict[str, Any]) -> None:
        total_restored_qty = 0
        for level in (1, 2, 3):
            price = to_int(row.get(f"복구매도가{level}"))
            qty = to_int(row.get(f"복구매도가{level}주문수량"))
            qty = min(qty, max(0, to_int(row.get("잔여 수량")) - total_restored_qty))
            if price > 0 and qty > 0:
                row[f"매도가{level}"] = price
                row[f"매도가{level}주문수량"] = qty
                self._send_sell_level_order(row, level, price, qty)
                total_restored_qty += qty
            else:
                self._clear_sell_level_order(row, level)
        self._sync_sell_order_numbers(row)
        row["손절복구매도가저장완료"] = False

    def _remaining_stop_limit_qty(self, row: Dict[str, Any]) -> int:
        ordered_qty = to_int(row.get("손절지정가주문수량"))
        filled_qty = to_int(row.get("손절지정가체결수량"))
        remaining = max(0, ordered_qty - filled_qty)
        holding_qty = to_int(row.get("잔여 수량"))
        return min(remaining if remaining > 0 else holding_qty, holding_qty)

    def _sell_level_count(self, row: Dict[str, Any]) -> int:
        return sum(
            1
            for level in (1, 2, 3)
            if to_int(row.get(f"매도가{level}")) > 0 or to_int(row.get(f"매도가{level}주문수량")) > 0
        )

    def _ensure_sell_level_metadata(self, row: Dict[str, Any]) -> None:
        if row.get("매도전략상태") in {
            SELL_STATUS_STOP_LIMIT_SWITCH,
            SELL_STATUS_STOP_LIMIT_READY,
            SELL_STATUS_STOP_LIMIT_ORDERED,
            SELL_STATUS_STOP_LIMIT_RESTORE,
            SELL_STATUS_STOP_LIMIT_TARGET_MARKET,
            SELL_STATUS_STOP_LIMIT_CLOSE_WAIT,
        }:
            return
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

    def _active_sell_order_count(self, row: Dict[str, Any]) -> int:
        count = 0
        for level in (1, 2, 3):
            if str(row.get(f"매도가{level}주문번호", "")) and self._remaining_sell_qty_by_level(row, level) > 0:
                count += 1
        return count

    def _has_pending_sell_order(self, row: Dict[str, Any]) -> bool:
        for level in (1, 2, 3):
            order_no = str(row.get(f"매도가{level}주문번호", ""))
            if self._is_pending_order_no(order_no) and self._remaining_sell_qty_by_level(row, level) > 0:
                return True
        return False

    def _is_pending_order_no(self, order_no: str) -> bool:
        return str(order_no).startswith("PENDING_")

    def _decrement_pending_sell_cancel_count(self, row: Dict[str, Any]) -> int:
        pending = to_int(row.get("매도취소대기수"))
        if pending <= 0:
            pending = 1
        pending = max(0, pending - 1)
        row["매도취소대기수"] = pending
        return pending

    def _has_active_sell_order(self, row: Dict[str, Any]) -> bool:
        for level in (1, 2, 3):
            if str(row.get(f"매도가{level}주문번호", "")) and self._remaining_sell_qty_by_level(row, level) > 0:
                return True
        return bool(str(row.get("매도주문번호들", "")))

    def _request_stop_limit_transition_if_needed(self, row: Dict[str, Any]) -> None:
        current_price = to_int(row.get("현재가"))
        stop_price = to_int(row.get("손절가"))
        holding_qty = to_int(row.get("잔여 수량"))
        if current_price <= 0 or stop_price <= 0 or holding_qty <= 0:
            return

        status = row.get("매도전략상태")
        if status == SELL_STATUS_STOP_LIMIT_READY:
            if current_price <= stop_price:
                self._complete_stop_limit_switch(row)
            return

        if current_price > stop_price * 1.02:
            return
        if status in {
            SELL_STATUS_CANCEL_REQUESTED,
            SELL_STATUS_MARKET_REQUESTED,
            SELL_STATUS_RELOCATION_CANCEL_REQUESTED,
            SELL_STATUS_RELOCATION_LEVEL1_CANCEL_REQUESTED,
            SELL_STATUS_STOP_LIMIT_SWITCH,
            SELL_STATUS_STOP_LIMIT_READY,
            SELL_STATUS_STOP_LIMIT_ORDERED,
            SELL_STATUS_STOP_LIMIT_RESTORE,
            SELL_STATUS_STOP_LIMIT_TARGET_MARKET,
            SELL_STATUS_STOP_LIMIT_CLOSE_WAIT,
        }:
            return

        self._remember_restore_sell_levels(row)
        row["청산사유"] = "손절 지정가"
        row["매도전략상태"] = SELL_STATUS_STOP_LIMIT_SWITCH
        self._remember_sell_cancel_requested(row)

        if self._has_active_sell_order(row):
            row["매도취소대기수"] = self._active_sell_order_count(row)
            if not self.kiwoom.cancel_sell_orders(row):
                self.sell_log(f"{self._name(row)}: 손절 지정가 전환 취소 요청 실패")
                row["매도전략상태"] = SELL_STRATEGY_RELOCATED if row.get("매도재배치여부") else SELL_STRATEGY_NORMAL
                row["매도취소대기수"] = 0
                return
            self.sell_log(f"{self._name(row)}: 손절 지정가 전환 - 기존 지정가 취소 요청")
            return

        self.kiwoom.cancel_sell_orders(row)
        self._complete_stop_limit_switch(row)

    def _complete_stop_limit_switch(self, row: Dict[str, Any]) -> None:
        qty = to_int(row.get("잔여 수량"))
        stop_price = to_int(row.get("손절가"))
        current_price = to_int(row.get("현재가"))
        if qty <= 0 or stop_price <= 0:
            row["매도전략상태"] = SELL_STRATEGY_RELOCATED if row.get("매도재배치여부") else SELL_STRATEGY_NORMAL
            return
        self._clear_all_sell_level_orders(row, keep_prices=True)
        if current_price > stop_price:
            row["손절지정가주문번호"] = ""
            row["손절지정가주문수량"] = 0
            row["손절지정가체결수량"] = 0
            row["매도전략상태"] = SELL_STATUS_STOP_LIMIT_READY
            row["매도취소대기수"] = 0
            self.sell_log(f"{self._name(row)}: 손절 지정가 대기 {stop_price:,}원/{qty}주")
            return
        row["손절지정가주문수량"] = qty
        row["손절지정가체결수량"] = to_int(row.get("손절지정가체결수량"))
        row["손절지정가요청중"] = True
        row["손절지정가주문번호"] = self.kiwoom.send_sell_order(row, stop_price, qty) or ""
        row["매도전략상태"] = SELL_STATUS_STOP_LIMIT_ORDERED
        row["매도취소대기수"] = 0
        self.sell_log(f"{self._name(row)}: 손절 지정가 주문 요청 {stop_price:,}원/{qty}주")

    def _request_stop_limit_target_market_if_needed(self, row: Dict[str, Any]) -> None:
        if row.get("매도전략상태") != SELL_STATUS_STOP_LIMIT_ORDERED:
            return
        if to_int(row.get("손절지정가체결수량")) > 0:
            return
        current_price = to_int(row.get("현재가"))
        stop_price = to_int(row.get("손절가"))
        if current_price <= 0 or stop_price <= 0:
            return
        if current_price >= stop_price * 1.035:
            return

        trigger = self._stop_limit_target_market_trigger(row, stop_price, current_price)
        if trigger is None:
            return
        level, target_price, target_qty = trigger
        if target_qty <= 0:
            return

        row["손절지정가목표시장가단계"] = level
        row["손절지정가목표시장가수량"] = target_qty
        row["손절지정가목표시장가가격"] = target_price
        row["청산사유"] = f"손절 지정가 중 매도가{level} 도달"
        row["매도전략상태"] = SELL_STATUS_STOP_LIMIT_TARGET_MARKET
        self._remember_sell_cancel_requested(row)

        order_no = str(row.get("손절지정가주문번호", ""))
        if not order_no:
            self._complete_stop_limit_target_market(row)
            return
        qty = self._remaining_stop_limit_qty(row)
        if not self.kiwoom.cancel_sell_order_qty(row, order_no, qty):
            row["매도전략상태"] = SELL_STATUS_STOP_LIMIT_ORDERED
            self.sell_log(f"{self._name(row)}: 손절 지정가 목표가 전환 취소 요청 실패")
            return
        self.sell_log(
            f"{self._name(row)}: 손절 지정가 취소 요청 - 매도가{level} 시장가 전환"
        )

    def _stop_limit_target_market_trigger(
        self,
        row: Dict[str, Any],
        stop_price: int,
        current_price: int,
    ) -> Optional[Tuple[int, int, int]]:
        lower = stop_price * 1.02
        upper = stop_price * 1.035
        for level in (1, 2, 3):
            target_price = to_int(row.get(f"복구매도가{level}"))
            if not (lower < target_price < upper):
                continue
            if current_price < target_price:
                continue
            qty = min(
                to_int(row.get(f"복구매도가{level}주문수량")),
                to_int(row.get("잔여 수량")),
            )
            if qty > 0:
                return level, target_price, qty
        return None

    def _complete_stop_limit_target_market(self, row: Dict[str, Any]) -> None:
        qty = min(
            to_int(row.pop("손절지정가목표시장가수량", 0)),
            to_int(row.get("잔여 수량")),
        )
        level = to_int(row.pop("손절지정가목표시장가단계", 0))
        target_price = to_int(row.pop("손절지정가목표시장가가격", 0))
        row["손절지정가주문번호"] = ""
        row["손절지정가주문수량"] = 0
        row["손절지정가체결수량"] = 0
        if qty <= 0:
            row["매도전략상태"] = SELL_STRATEGY_RELOCATED if row.get("매도재배치여부") else SELL_STRATEGY_NORMAL
            return
        order_no = self.kiwoom.send_market_sell_order(row, qty)
        self._append_market_order_no(row, order_no)
        row["매도전략상태"] = SELL_STATUS_MARKET_REQUESTED
        row["손절지정가목표시장가주문중"] = True
        self.sell_log(
            f"{self._name(row)}: 매도가{level} {target_price:,}원 도달 시장가 매도 요청 {qty}주"
        )

    def _request_stop_limit_restore_if_needed(self, row: Dict[str, Any]) -> None:
        if row.get("매도전략상태") not in {SELL_STATUS_STOP_LIMIT_READY, SELL_STATUS_STOP_LIMIT_ORDERED}:
            return
        if to_int(row.get("손절지정가체결수량")) > 0:
            return
        current_price = to_int(row.get("현재가"))
        stop_price = to_int(row.get("손절가"))
        if current_price <= 0 or stop_price <= 0:
            return
        if current_price < stop_price * 1.035:
            return

        order_no = str(row.get("손절지정가주문번호", ""))
        if row.get("매도전략상태") == SELL_STATUS_STOP_LIMIT_READY:
            self._complete_stop_limit_restore(row)
            return
        if not order_no:
            self.kiwoom.cancel_sell_orders(row)
            self._complete_stop_limit_restore(row)
            return
        row["매도전략상태"] = SELL_STATUS_STOP_LIMIT_RESTORE
        self._remember_sell_cancel_requested(row)
        qty = self._remaining_stop_limit_qty(row)
        if not self.kiwoom.cancel_sell_order_qty(row, order_no, qty):
            self.sell_log(f"{self._name(row)}: 손절 지정가 복구 취소 요청 실패")
            row["매도전략상태"] = SELL_STATUS_STOP_LIMIT_ORDERED
            return
        self.sell_log(f"{self._name(row)}: 손절 지정가 취소 요청 - 매도가 복구")

    def _complete_stop_limit_restore(self, row: Dict[str, Any]) -> None:
        if to_int(row.get("손절지정가체결수량")) > 0:
            row["매도전략상태"] = SELL_STATUS_STOP_LIMIT_ORDERED
            return
        row["손절지정가주문번호"] = ""
        row["손절지정가주문수량"] = 0
        row["손절지정가체결수량"] = 0
        self._restore_sell_levels(row)
        row["매도전략상태"] = SELL_STRATEGY_RELOCATED if row.get("매도재배치여부") else SELL_STRATEGY_NORMAL
        self.sell_log(f"{self._name(row)}: 손절 지정가 취소 완료 - 매도가 복구")

    def _request_stop_limit_close_wait_if_needed(self, row: Dict[str, Any], now: datetime) -> None:
        if now.time() < time(15, 20, 0):
            return
        if row.get("매도전략상태") != SELL_STATUS_STOP_LIMIT_ORDERED:
            return
        if to_int(row.get("잔여 수량")) <= 0:
            return
        order_no = str(row.get("손절지정가주문번호", ""))
        if not order_no:
            self.kiwoom.cancel_sell_orders(row)
            row["매도전략상태"] = SELL_STRATEGY_RELOCATED if row.get("매도재배치여부") else SELL_STRATEGY_NORMAL
            return
        row["매도전략상태"] = SELL_STATUS_STOP_LIMIT_CLOSE_WAIT
        self._remember_sell_cancel_requested(row)
        qty = self._remaining_stop_limit_qty(row)
        if not self.kiwoom.cancel_sell_order_qty(row, order_no, qty):
            self.sell_log(f"{self._name(row)}: 15:20 손절 지정가 취소 요청 실패")
            row["매도전략상태"] = SELL_STATUS_STOP_LIMIT_ORDERED
            return
        self.sell_log(f"{self._name(row)}: 15:20 손절 지정가 취소 요청 - 장마감 청산 대기")

    def _request_profit_market_liquidation_if_needed(self, row: Dict[str, Any]) -> None:
        self._request_stop_limit_transition_if_needed(row)

    def _move_to_list3(self, row: Dict[str, Any]) -> None:
        if self._should_sync_trade_fills_before_summary(row):
            if self.kiwoom.request_trade_fill_sync(row, "매매요약 전 보정"):
                row["매매요약대기"] = True
                self.refresh_tables()
                return
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
        input_buy_price = row.get("재배치 기준 매수가", row.get("매수가"))
        actual_buy_price = row.get("실제 매수가") or row.get("매수가")
        return {
            "종목코드": row.get("종목코드", ""),
            "종목명": row.get("종목명", ""),
            "매수가": actual_buy_price,
            "재배치 기준 매수가": input_buy_price,
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
            "매수체결금액": row.get("매수체결금액", 0),
            "매도주문번호들": "",
            "매도전략상태": SELL_STRATEGY_NORMAL,
            "매도취소대기수": 0,
            "매도가1체결수량": 0,
            "매도가2체결수량": 0,
            "매도가3체결수량": 0,
            "매도 수량": 0,
            "시장가주문번호": "",
            "청산사유": "",
            "손절지정가주문번호": "",
            "손절지정가주문수량": 0,
            "손절지정가체결수량": 0,
            "손절복구매도가저장완료": False,
            "복구매도가1": row.get("매도가1"),
            "복구매도가2": row.get("매도가2"),
            "복구매도가3": row.get("매도가3"),
            "복구매도가1주문수량": 0,
            "복구매도가2주문수량": 0,
            "복구매도가3주문수량": 0,
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
            "직전저가": row.get("저가"),
            "11시부분청산완료": False,
            "12시저가부분청산완료": False,
            "장마감청산요청완료": False,
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





