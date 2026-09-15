# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""
Instrument catalog loading for the Binance adapter.
"""

import asyncio

from nautilus_trader._libnautilus.binance import BinanceDataClientConfig
from nautilus_trader._libnautilus.binance import BinanceProductType
from nautilus_trader._libnautilus.binance import _decode_binance_klines
from nautilus_trader._libnautilus.binance import _load_binance_instruments
from nautilus_trader._libnautilus.binance import _parse_binance_instrument


BinanceKlineRow = tuple[int, str, str, str, str, str, int, int]


def decode_binance_klines(payload: bytes) -> list[BinanceKlineRow]:
    """
    Decode raw Binance REST kline JSON through the adapter's native schema.

    The tuple fields are ``open_time``, ``open``, ``high``, ``low``, ``close``,
    ``volume``, ``close_time``, and ``trade_count``. This function performs no
    network I/O, so callers can parse the exact response bytes they audited.

    """
    return _decode_binance_klines(payload)


def parse_binance_instrument(
    payload: bytes,
    product_type: BinanceProductType,
    symbol: str,
    ts_init_ns: int,
) -> object:
    """
    Parse one native instrument from raw Binance exchange-info JSON.

    The native Spot, USD-M, or COIN-M parser is selected by ``product_type``.
    Both Nautilus observation timestamps use the caller-supplied ``ts_init_ns``.
    The function is transport-free and never issues a second HTTP request.

    """
    return _parse_binance_instrument(payload, product_type, symbol, ts_init_ns)


async def load_binance_instruments(config: BinanceDataClientConfig) -> list[object]:
    """
    Load the configured Binance instrument catalog.

    This is the Python v2 replacement for constructing a cached low-level HTTP client
    and a product-specific v1 instrument provider. The embedded ``instrument_provider``
    config controls selection, filters, parser warnings, and commission queries.

    """
    return await asyncio.to_thread(_load_binance_instruments, config)
