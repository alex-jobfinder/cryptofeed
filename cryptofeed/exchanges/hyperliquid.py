# !hyperliquid.py
from cryptofeed.symbols import Symbol
from cryptofeed.util.time import timedelta_str_to_sec
import logging
from typing import Dict, List, Tuple
from decimal import Decimal

from yapic import json

from cryptofeed.symbols import Symbol, Symbols
from cryptofeed.connection import AsyncConnection, WebsocketEndpoint
from cryptofeed.defines import BUY, SELL, TRADES, L2_BOOK, HYPERLIQUID, CANDLES
from cryptofeed.feed import Feed
from cryptofeed.types import OrderBook, Trade, Candle

LOG = logging.getLogger('feedhandler')


class HyperLiquid(Feed):
    id = HYPERLIQUID
    websocket_endpoints = [
        WebsocketEndpoint('wss://api.hyperliquid.xyz/ws', options={'compression': None}),
        WebsocketEndpoint('wss://api.hyperliquid-testnet.xyz/ws', options={'compression': None})
    ]
    rest_endpoints = []
    valid_candle_intervals = {'1m', '5m', '15m', '1h'}
    candle_interval_map = {k: k for k in valid_candle_intervals}
    websocket_channels = {
        L2_BOOK: 'l2Book',
        TRADES: 'trades',
        CANDLES: 'candle'
    }
    symbol_endpoint = 'https://api.hyperliquid.xyz/info'

    @classmethod
    def timestamp_normalize(cls, ts: float | int) -> Decimal:
        return Decimal(str(ts)) / Decimal("1000.0")

    @classmethod
    def symbol_mapping(cls, refresh=False):
        if Symbols.populated(cls.id) and not refresh:
            return Symbols.get(cls.id)[0]

        try:
            response = cls.http_sync.write(
                cls.symbol_endpoint,
                data={"type": "allMids"},
                is_data_json=True,
                json=True
            )

            syms, info = cls._parse_symbol_data(response)
            LOG.debug(f"HyperLiquid symbol mapping result: {syms}")
            Symbols.set(cls.id, syms, info)
            return syms
        except Exception as e:
            LOG.error("%s: Failed to parse symbol information: %s", cls.id, str(e), exc_info=True)
            raise

    @classmethod
    def _parse_symbol_data(cls, data: dict = None) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
        symbols_map: Dict[str, str] = {}
        info: Dict[str, Dict[str, str]] = {'instrument_type': {}}

        if not data:
            LOG.error("HyperLiquid info endpoint returned no data or invalid response")
            return symbols_map, info

        if isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception as e:
                LOG.error(f"Failed to parse JSON from HyperLiquid response: {e}")
                return symbols_map, info

        if not isinstance(data, dict):
            LOG.error("Unexpected format from HyperLiquid info endpoint (expected dict of symbols)")
            return symbols_map, info

        for sym in data.keys():
            if sym.startswith('@'):
                continue
            symbols_map[sym] = sym
            info['instrument_type'][sym] = 'perp'

        return symbols_map, info

    def __reset(self):
        self._l2_book: Dict[str, OrderBook] = {}

    async def subscribe(self, conn: AsyncConnection):
        self.__reset()
        for chan in self.websocket_channels:
            for pair in self.subscription.get(chan, []):
                symbol = self.std_symbol_to_exchange_symbol(pair)
                LOG.debug(f"HyperLiquid subscribe: channel={chan}, std_symbol={pair}, exchange_symbol={symbol}")

                if chan == CANDLES:
                    for interval in self.subscription_interval[chan][pair]:
                        sub_msg = {
                            "method": "subscribe",
                            "subscription": {
                                "type": self.websocket_channels[chan],
                                "coin": symbol,
                                "interval": interval
                            }
                        }
                        LOG.debug(f"Sending candle subscribe message: {sub_msg}")
                        await conn.write(json.dumps(sub_msg))
                else:
                    sub_msg = {
                        "method": "subscribe",
                        "subscription": {
                            "type": self.websocket_channels[chan],
                            "coin": symbol
                        }
                    }
                    LOG.debug(f"Sending subscribe message: {sub_msg}")
                    await conn.write(json.dumps(sub_msg))

    async def message_handler(self, msg: str, conn, timestamp: float):
        LOG.debug(f"HyperLiquid raw message received: {msg}")
        msg = json.loads(msg, parse_float=Decimal)

        if msg.get("channel") == "trades":
            data = msg.get("data", [])
            if not data:
                LOG.warning(f"HyperLiquid: 'trades' message has no data: {msg}")
            for trade in data:
                coin = trade.get("coin")
                side = trade.get("side")
                price = trade.get("px")
                size = trade.get("sz")
                LOG.debug(f"Parsing trade: coin={coin}, side={side}, price={price}, size={size}, raw={trade}")

                users = trade.get("users", [])

                if not isinstance(users, list) or len(users) < 2:
                    initiator = "missing"
                    counterparty = "missing"
                else:
                    if side == "B":
                        initiator = users[0]
                        counterparty = users[1]
                    else:
                        initiator = users[1]
                        counterparty = users[0]

                std_symbol = self.exchange_symbol_to_std_symbol(coin)
                LOG.debug(f"HyperLiquid: Mapped exchange symbol '{coin}' to std symbol '{std_symbol}'")

                trade_time = trade.get("time", 0)
                enriched_trade = {
                    **trade,
                    "initiator": initiator,
                    "counterparty": counterparty
                }

                t = Trade(
                    self.id,
                    std_symbol,
                    BUY if side == "B" else SELL,
                    Decimal(size),
                    Decimal(price),
                    self.timestamp_normalize(trade_time),
                    id=trade.get("hash"),
                    raw=enriched_trade
                )
                LOG.debug(f"HyperLiquid: Constructed Trade event: {t}")
                await self.callback(TRADES, t, timestamp)
                LOG.debug(f"HyperLiquid: TRADES callback executed for {std_symbol}")

        else:
            LOG.debug(f"HyperLiquid: Unhandled message type {msg.get('channel')}: {msg}")






# '''
# Copyright (C) 2017-2025 Bryant Moscon - bmoscon@gmail.com

# Please see the LICENSE file for the terms and conditions
# associated with this software.
# '''

# # !hyperliquid.py
# from cryptofeed.symbols import Symbol
# from cryptofeed.util.time import timedelta_str_to_sec
# import logging
# from typing import Dict, List, Tuple
# from decimal import Decimal

# from yapic import json

# from cryptofeed.symbols import Symbol, Symbols
# from cryptofeed.connection import AsyncConnection, WebsocketEndpoint
# from cryptofeed.defines import BUY, SELL, TRADES, L2_BOOK, HYPERLIQUID, CANDLES
# from cryptofeed.feed import Feed
# from cryptofeed.types import OrderBook, Trade, Candle

# LOG = logging.getLogger('feedhandler')


# class HyperLiquid(Feed):
#     id = HYPERLIQUID
#     websocket_endpoints = [
#         WebsocketEndpoint('wss://api.hyperliquid.xyz/ws', options={'compression': None}),
#         WebsocketEndpoint('wss://api.hyperliquid-testnet.xyz/ws', options={'compression': None})
#     ]
#     rest_endpoints = []
#     valid_candle_intervals = {'1m', '5m', '15m', '1h'}
#     candle_interval_map = {k: k for k in valid_candle_intervals}
#     # candle_interval_map = {'1m': '1min', '3m': '3min', '15m': '15min', '30m': '30min', '1h': '1hour', '2h': '2hour', '4h': '4hour', '6h': '6hour', '8h': '8hour', '12h': '12hour', '1d': '1day', '1w': '1week'}
#     websocket_channels = {
#         L2_BOOK: 'l2Book',
#         TRADES: 'trades',
#         CANDLES: 'candle'
#     }
#     symbol_endpoint = 'https://api.hyperliquid.xyz/info'  # Info endpoint URL (will use POST)

#     @classmethod
#     def timestamp_normalize(cls, ts: float) -> float:
#         return ts / 1000.0


#     @classmethod
#     def symbol_mapping(cls, refresh=False):
#         if Symbols.populated(cls.id) and not refresh:
#             return Symbols.get(cls.id)[0]

#         try:
#             response = cls.http_sync.write(
#                 cls.symbol_endpoint,
#                 data={"type": "allMids"},
#                 is_data_json=True,
#                 json=True
#             )

#             syms, info = cls._parse_symbol_data(response)
#             Symbols.set(cls.id, syms, info)
#             return syms
#         except Exception as e:
#             LOG.error("%s: Failed to parse symbol information: %s", cls.id, str(e), exc_info=True)
#             raise

#     @classmethod
#     def _parse_symbol_data(cls, data: dict = None) -> Tuple[Dict[str, str], Dict[str, Dict[str, str]]]:
#         symbols_map: Dict[str, str] = {}
#         info: Dict[str, Dict[str, str]] = {'instrument_type': {}}

#         if not data:
#             LOG.error("HyperLiquid info endpoint returned no data or invalid response")
#             return symbols_map, info

#         if isinstance(data, str):
#             try:
#                 data = json.loads(data)
#             except Exception as e:
#                 LOG.error(f"Failed to parse JSON from HyperLiquid response: {e}")
#                 return symbols_map, info

#         if not isinstance(data, dict):
#             LOG.error("Unexpected format from HyperLiquid info endpoint (expected dict of symbols)")
#             return symbols_map, info

#         for sym in data.keys():
#             if sym.startswith('@'):
#                 continue
#             symbols_map[sym] = sym
#             info['instrument_type'][sym] = 'perp'

#         return symbols_map, info

#     def __reset(self):
#         self._l2_book: Dict[str, OrderBook] = {}

#     async def subscribe(self, conn: AsyncConnection):
#         self.__reset()
#         for chan in self.websocket_channels:
#             for pair in self.subscription.get(chan, []):
#                 symbol = self.std_symbol_to_exchange_symbol(pair)

#                 if chan == CANDLES:
#                     for interval in self.subscription_interval[chan][pair]:
#                         sub_msg = {
#                             "type": self.websocket_channels[chan],
#                             "coin": symbol,
#                             "interval": interval
#                         }
#                         # await conn.write(json.dumps(sub_msg), is_data_json=True)
#                         await conn.write(json.dumps(sub_msg))
#                 else:
#                     sub_msg = {"type": self.websocket_channels[chan], "coin": symbol}
#                     # await conn.write(json.dumps(sub_msg), is_data_json=True)
#                     await conn.write(json.dumps(sub_msg))

#     async def message_handler(self, msg: str, conn, timestamp: float):
#         msg = json.loads(msg, parse_float=Decimal)

#         if msg.get("channel") == "trades":
#             for trade in msg["data"]:
#                 users = trade.get("users", [])

#                 if not isinstance(users, list) or len(users) < 2:
#                     initiator = "missing"
#                     counterparty = "missing"
#                 else:
#                     # Per schema: users[0] = buyer, users[1] = seller
#                     if trade.get("side") == "B":
#                         initiator = users[0]
#                         counterparty = users[1]
#                     else:
#                         initiator = users[1]
#                         counterparty = users[0]

#                 enriched_trade = {
#                     **trade,
#                     "initiator": initiator,
#                     "counterparty": counterparty
#                 }

#                 t = Trade(
#                     self.id,
#                     self.exchange_symbol_to_std_symbol(trade["coin"]),
#                     BUY if trade["side"] == "B" else SELL,
#                     Decimal(trade["sz"]),
#                     Decimal(trade["px"]),
#                     self.timestamp_normalize(trade["time"]),
#                     id=trade["hash"],
#                     raw=enriched_trade  # enriched version with initiator/counterparty
#                 )
#                 await self.callback(TRADES, t, timestamp)


#         elif msg.get("channel") == "l2Book":
#             pair = self.exchange_symbol_to_std_symbol(msg["data"]["coin"])
#             if pair not in self._l2_book:
#                 self._l2_book[pair] = OrderBook(self.id, pair, max_depth=self.max_depth)

#             bids_raw = msg["data"]["levels"][0]
#             asks_raw = msg["data"]["levels"][1]

#             bids = {Decimal(level["px"]): Decimal(level["sz"]) for level in bids_raw}
#             asks = {Decimal(level["px"]): Decimal(level["sz"]) for level in asks_raw}

#             ob = self._l2_book[pair]
#             ob.book.bids = bids
#             ob.book.asks = asks
#             ob.timestamp = self.timestamp_normalize(msg["data"]["time"])
#             ob.raw = msg

#             await self.book_callback(L2_BOOK, ob, timestamp, timestamp=ob.timestamp, raw=msg)


#         elif msg.get("channel") == "candle":
#             for candle in msg["data"]:
#                 c = Candle(
#                     self.id,
#                     self.exchange_symbol_to_std_symbol(candle["s"]),
#                     start=self.timestamp_normalize(candle["t"]),
#                     stop=self.timestamp_normalize(candle["T"]),
#                     interval=candle["i"],
#                     open=Decimal(candle["o"]),
#                     close=Decimal(candle["c"]),
#                     high=Decimal(candle["h"]),
#                     low=Decimal(candle["l"]),
#                     volume=Decimal(candle["v"]),
#                     trades=int(candle["n"]),
#                     timestamp=timestamp,
#                     raw=candle
#                 )
#                 await self.callback(CANDLES, c, timestamp)