// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

use nautilus_common::live::get_runtime;
use nautilus_core::{UnixNanos, python::to_pyvalue_err, time::get_atomic_clock_realtime};
use nautilus_model::python::instruments::instrument_any_to_pyobject;
use pyo3::{prelude::*, types::PyList};

use crate::{
    common::{
        enums::BinanceProductType,
        parse::{
            parse_coinm_instrument, parse_spot_instrument_json_with_fees, parse_usdm_instrument,
        },
        urls::get_http_base_url_with_us,
    },
    config::BinanceDataClientConfig,
    futures::http::{
        client::BinanceFuturesHttpClient,
        models::{
            BinanceFuturesCoinExchangeInfo, BinanceFuturesKline, BinanceFuturesUsdExchangeInfo,
        },
    },
    spot::http::{client::BinanceSpotHttpClient, models::BinanceExchangeInfoJson},
};

type BinanceKlineRow = (i64, String, String, String, String, String, i64, i64);

/// Decodes raw Binance REST kline JSON through the adapter's native schema.
///
/// The returned tuple fields are open time, open, high, low, close, volume, close time, and
/// trade count. The function is deliberately transport-free so an audited caller can parse the
/// exact response bytes it already persisted without issuing a second HTTP request.
///
/// # Errors
///
/// Returns an error if the payload does not match the native Binance kline schema.
#[pyfunction]
#[pyo3(name = "_decode_binance_klines")]
pub(super) fn py_decode_binance_klines(payload: &[u8]) -> PyResult<Vec<BinanceKlineRow>> {
    let rows: Vec<BinanceFuturesKline> = serde_json::from_slice(payload).map_err(to_pyvalue_err)?;

    Ok(rows
        .into_iter()
        .map(|row| {
            (
                row.open_time,
                row.open,
                row.high,
                row.low,
                row.close,
                row.volume,
                row.close_time,
                row.num_trades,
            )
        })
        .collect())
}

/// Parses one instrument from already-fetched Binance exchange-info JSON.
///
/// This transport-free boundary reuses the adapter's native Spot, USD-M, and COIN-M instrument
/// parsers. Both Nautilus observation timestamps are set to `ts_init_ns`, which must be supplied
/// by the audited caller that owns the HTTP response.
///
/// # Errors
///
/// Returns an error if the product type is unsupported, the payload is invalid, the requested
/// symbol is absent, or the native instrument parser rejects the symbol definition.
#[pyfunction]
#[pyo3(name = "_parse_binance_instrument")]
pub(super) fn py_parse_binance_instrument(
    py: Python<'_>,
    payload: &[u8],
    product_type: BinanceProductType,
    symbol: &str,
    ts_init_ns: u64,
) -> PyResult<Py<PyAny>> {
    let timestamp = UnixNanos::from(ts_init_ns);
    let instrument = match product_type {
        BinanceProductType::Spot => {
            let exchange_info: BinanceExchangeInfoJson =
                serde_json::from_slice(payload).map_err(to_pyvalue_err)?;
            let symbol_info = exchange_info
                .symbols
                .iter()
                .find(|item| item.symbol == symbol)
                .ok_or_else(|| {
                    to_pyvalue_err(format!(
                        "Binance Spot exchange info does not contain symbol '{symbol}'"
                    ))
                })?;
            parse_spot_instrument_json_with_fees(symbol_info, None, None, timestamp, timestamp)
                .map_err(to_pyvalue_err)?
        }
        BinanceProductType::UsdM => {
            let exchange_info: BinanceFuturesUsdExchangeInfo =
                serde_json::from_slice(payload).map_err(to_pyvalue_err)?;
            let symbol_info = exchange_info
                .symbols
                .iter()
                .find(|item| item.symbol.as_str() == symbol)
                .ok_or_else(|| {
                    to_pyvalue_err(format!(
                        "Binance USD-M exchange info does not contain symbol '{symbol}'"
                    ))
                })?;
            parse_usdm_instrument(symbol_info, timestamp, timestamp).map_err(to_pyvalue_err)?
        }
        BinanceProductType::CoinM => {
            let exchange_info: BinanceFuturesCoinExchangeInfo =
                serde_json::from_slice(payload).map_err(to_pyvalue_err)?;
            let symbol_info = exchange_info
                .symbols
                .iter()
                .find(|item| item.symbol.as_str() == symbol)
                .ok_or_else(|| {
                    to_pyvalue_err(format!(
                        "Binance COIN-M exchange info does not contain symbol '{symbol}'"
                    ))
                })?;
            parse_coinm_instrument(symbol_info, timestamp, timestamp).map_err(to_pyvalue_err)?
        }
        product_type => {
            return Err(to_pyvalue_err(format!(
                "Binance offline instrument parsing supports Spot, UsdM, or CoinM, was {product_type:?}"
            )));
        }
    };

    instrument_any_to_pyobject(py, instrument)
}

/// Loads the configured Binance instrument catalog for the Python async facade.
///
/// The public `load_binance_instruments` coroutine runs this blocking boundary in a Python worker
/// thread. The request uses the same domain-level HTTP paths and
/// [`crate::config::BinanceInstrumentProviderConfig`] as the live data client.
///
/// # Errors
///
/// Returns an error if the configuration is invalid, the product type is unsupported, the
/// catalog request fails, or an instrument cannot be converted to Python.
#[pyfunction]
#[pyo3(name = "_load_binance_instruments")]
pub(super) fn py_load_binance_instruments<'py>(
    py: Python<'py>,
    config: BinanceDataClientConfig,
) -> PyResult<Bound<'py, PyList>> {
    config.validate().map_err(to_pyvalue_err)?;

    let instruments = py
        .detach(|| {
            get_runtime().block_on(async move {
                let api_key = config
                    .api_key
                    .as_ref()
                    .map(|value| value.expose_secret().to_owned());
                let api_secret = config
                    .api_secret
                    .as_ref()
                    .map(|value| value.expose_secret().to_owned());
                let proxy_url = config
                    .proxy_url
                    .as_ref()
                    .map(|value| value.expose_secret().to_owned());

                match config.product_type {
                    BinanceProductType::Spot => {
                        let base_url_http = config.base_url_http.clone().or_else(|| {
                            config.us.then(|| {
                                get_http_base_url_with_us(
                                    config.product_type,
                                    config.environment,
                                    true,
                                )
                                .to_string()
                            })
                        });
                        let client = BinanceSpotHttpClient::new_with_json_responses(
                            config.environment,
                            get_atomic_clock_realtime(),
                            api_key.clone(),
                            api_secret.clone(),
                            base_url_http,
                            Some(config.recv_window_ms),
                            None,
                            proxy_url.clone(),
                            config.us,
                        )
                        .map_err(|e| e.to_string())?;

                        client
                            .request_instruments_with_config(
                                &config.instrument_provider,
                                config.us,
                            )
                            .await
                            .map_err(|e| e.to_string())
                    }
                    BinanceProductType::UsdM | BinanceProductType::CoinM => {
                        let client = BinanceFuturesHttpClient::new(
                            config.product_type,
                            config.environment,
                            get_atomic_clock_realtime(),
                            api_key,
                            api_secret,
                            config.base_url_http.clone(),
                            Some(config.recv_window_ms),
                            None,
                            proxy_url,
                            false,
                        )
                        .map_err(|e| e.to_string())?;

                        client
                            .request_instruments_with_config(&config.instrument_provider)
                            .await
                            .map_err(|e| e.to_string())
                    }
                    product_type => Err(format!(
                        "Binance instrument loading supports Spot, UsdM, or CoinM, was {product_type:?}"
                    )),
                }
            })
        })
        .map_err(to_pyvalue_err)?;
    let instruments = instruments
        .into_iter()
        .map(|instrument| instrument_any_to_pyobject(py, instrument))
        .collect::<PyResult<Vec<_>>>()?;

    PyList::new(py, instruments)
}
