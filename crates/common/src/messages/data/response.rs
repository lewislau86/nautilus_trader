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

use std::{any::Any, sync::Arc};

use nautilus_core::{Params, UUID4, UnixNanos};
use nautilus_model::{
    data::{
        Bar, BarType, DataType, FundingRateUpdate, HasTsInit, OrderBookDelta, OrderBookDepth10,
        QuoteTick, TradeTick,
    },
    identifiers::{ClientId, InstrumentId, OptionSeriesId, Venue},
    instruments::InstrumentAny,
    orderbook::OrderBook,
    types::Price,
};
use serde::{Deserialize, Serialize};

use super::{Payload, RequestBars};

/// Trims `data` to the inclusive `[start, end]` window on `ts_init`.
///
/// When `start` is set, drops leading entries with `ts_init < start`; when `end`
/// is set, drops trailing entries with `ts_init > end`. Empty payloads and
/// absent bounds short-circuit. When the bounds do not overlap the payload
/// (e.g. `start` after the last entry, or `end` before the first), `data` is
/// cleared.
pub(crate) fn trim_data_to_bounds<T: HasTsInit>(
    data: &mut Vec<T>,
    start: Option<UnixNanos>,
    end: Option<UnixNanos>,
) {
    let data_len = data.len();
    if data_len == 0 {
        return;
    }

    let first_index = if let Some(start) = start {
        let Some(i) = data
            .iter()
            .position(|item| item.ts_init().as_u64() >= start.as_u64())
        else {
            data.clear();
            return;
        };
        i
    } else {
        0
    };

    let last_index = if let Some(end) = end {
        let Some(i) = data
            .iter()
            .rposition(|item| item.ts_init().as_u64() <= end.as_u64())
        else {
            data.clear();
            return;
        };
        i
    } else {
        data_len - 1
    };

    if first_index <= last_index {
        data.drain(..first_index);
        data.truncate(last_index - first_index + 1);
    } else {
        data.clear();
    }
}

#[derive(Clone, Debug)]
pub struct CustomDataResponse {
    pub correlation_id: UUID4,
    pub client_id: ClientId,
    pub venue: Option<Venue>,
    pub data_type: DataType,
    pub data: Payload,
    pub start: Option<UnixNanos>,
    pub end: Option<UnixNanos>,
    pub ts_init: UnixNanos,
    pub params: Option<Params>,
}

impl CustomDataResponse {
    /// Creates a new [`CustomDataResponse`] instance.
    #[expect(clippy::too_many_arguments)]
    pub fn new<T: Any + Send + Sync>(
        correlation_id: UUID4,
        client_id: ClientId,
        venue: Option<Venue>,
        data_type: DataType,
        data: T,
        start: Option<UnixNanos>,
        end: Option<UnixNanos>,
        ts_init: UnixNanos,
        params: Option<Params>,
    ) -> Self {
        Self {
            correlation_id,
            client_id,
            venue,
            data_type,
            data: Arc::new(data),
            start,
            end,
            ts_init,
            params,
        }
    }

    /// Converts the response to a dyn Any trait object for messaging.
    pub fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstrumentResponse {
    pub correlation_id: UUID4,
    pub client_id: ClientId,
    pub instrument_id: InstrumentId,
    pub data: InstrumentAny,
    pub start: Option<UnixNanos>,
    pub end: Option<UnixNanos>,
    pub ts_init: UnixNanos,
    pub params: Option<Params>,
}

impl InstrumentResponse {
    /// Converts to a dyn Any trait object for messaging.
    pub fn as_any(&self) -> &dyn Any {
        self
    }

    /// Creates a new [`InstrumentResponse`] instance.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        correlation_id: UUID4,
        client_id: ClientId,
        instrument_id: InstrumentId,
        data: InstrumentAny,
        start: Option<UnixNanos>,
        end: Option<UnixNanos>,
        ts_init: UnixNanos,
        params: Option<Params>,
    ) -> Self {
        Self {
            correlation_id,
            client_id,
            instrument_id,
            data,
            start,
            end,
            ts_init,
            params,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstrumentsResponse {
    pub correlation_id: UUID4,
    pub client_id: ClientId,
    pub venue: Venue,
    pub data: Vec<InstrumentAny>,
    pub start: Option<UnixNanos>,
    pub end: Option<UnixNanos>,
    pub ts_init: UnixNanos,
    pub params: Option<Params>,
}

impl InstrumentsResponse {
    /// Converts to a dyn Any trait object for messaging.
    pub fn as_any(&self) -> &dyn Any {
        self
    }

    /// Creates a new [`InstrumentsResponse`] instance.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        correlation_id: UUID4,
        client_id: ClientId,
        venue: Venue,
        data: Vec<InstrumentAny>,
        start: Option<UnixNanos>,
        end: Option<UnixNanos>,
        ts_init: UnixNanos,
        params: Option<Params>,
    ) -> Self {
        Self {
            correlation_id,
            client_id,
            venue,
            data,
            start,
            end,
            ts_init,
            params,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BookResponse {
    pub correlation_id: UUID4,
    pub client_id: ClientId,
    pub instrument_id: InstrumentId,
    pub data: OrderBook,
    pub start: Option<UnixNanos>,
    pub end: Option<UnixNanos>,
    pub ts_init: UnixNanos,
    pub params: Option<Params>,
}

impl BookResponse {
    /// Converts to a dyn Any trait object for messaging.
    pub fn as_any(&self) -> &dyn Any {
        self
    }

    /// Creates a new [`BookResponse`] instance.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        correlation_id: UUID4,
        client_id: ClientId,
        instrument_id: InstrumentId,
        data: OrderBook,
        start: Option<UnixNanos>,
        end: Option<UnixNanos>,
        ts_init: UnixNanos,
        params: Option<Params>,
    ) -> Self {
        Self {
            correlation_id,
            client_id,
            instrument_id,
            data,
            start,
            end,
            ts_init,
            params,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BookDeltasResponse {
    pub correlation_id: UUID4,
    pub client_id: ClientId,
    pub instrument_id: InstrumentId,
    pub data: Vec<OrderBookDelta>,
    pub start: Option<UnixNanos>,
    pub end: Option<UnixNanos>,
    pub ts_init: UnixNanos,
    pub params: Option<Params>,
}

impl BookDeltasResponse {
    /// Converts to a dyn Any trait object for messaging.
    pub fn as_any(&self) -> &dyn Any {
        self
    }

    /// Creates a new [`BookDeltasResponse`] instance.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        correlation_id: UUID4,
        client_id: ClientId,
        instrument_id: InstrumentId,
        data: Vec<OrderBookDelta>,
        start: Option<UnixNanos>,
        end: Option<UnixNanos>,
        ts_init: UnixNanos,
        params: Option<Params>,
    ) -> Self {
        Self {
            correlation_id,
            client_id,
            instrument_id,
            data,
            start,
            end,
            ts_init,
            params,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BookDepthResponse {
    pub correlation_id: UUID4,
    pub client_id: ClientId,
    pub instrument_id: InstrumentId,
    pub data: Vec<OrderBookDepth10>,
    pub start: Option<UnixNanos>,
    pub end: Option<UnixNanos>,
    pub ts_init: UnixNanos,
    pub params: Option<Params>,
}

impl BookDepthResponse {
    /// Converts to a dyn Any trait object for messaging.
    pub fn as_any(&self) -> &dyn Any {
        self
    }

    /// Creates a new [`BookDepthResponse`] instance.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        correlation_id: UUID4,
        client_id: ClientId,
        instrument_id: InstrumentId,
        data: Vec<OrderBookDepth10>,
        start: Option<UnixNanos>,
        end: Option<UnixNanos>,
        ts_init: UnixNanos,
        params: Option<Params>,
    ) -> Self {
        Self {
            correlation_id,
            client_id,
            instrument_id,
            data,
            start,
            end,
            ts_init,
            params,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QuotesResponse {
    pub correlation_id: UUID4,
    pub client_id: ClientId,
    pub instrument_id: InstrumentId,
    pub data: Vec<QuoteTick>,
    pub start: Option<UnixNanos>,
    pub end: Option<UnixNanos>,
    pub ts_init: UnixNanos,
    pub params: Option<Params>,
}

impl QuotesResponse {
    /// Converts to a dyn Any trait object for messaging.
    pub fn as_any(&self) -> &dyn Any {
        self
    }

    /// Creates a new [`QuotesResponse`] instance.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        correlation_id: UUID4,
        client_id: ClientId,
        instrument_id: InstrumentId,
        data: Vec<QuoteTick>,
        start: Option<UnixNanos>,
        end: Option<UnixNanos>,
        ts_init: UnixNanos,
        params: Option<Params>,
    ) -> Self {
        Self {
            correlation_id,
            client_id,
            instrument_id,
            data,
            start,
            end,
            ts_init,
            params,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TradesResponse {
    pub correlation_id: UUID4,
    pub client_id: ClientId,
    pub instrument_id: InstrumentId,
    pub data: Vec<TradeTick>,
    pub start: Option<UnixNanos>,
    pub end: Option<UnixNanos>,
    pub ts_init: UnixNanos,
    pub params: Option<Params>,
}

impl TradesResponse {
    /// Converts to a dyn Any trait object for messaging.
    pub fn as_any(&self) -> &dyn Any {
        self
    }

    /// Creates a new [`TradesResponse`] instance.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        correlation_id: UUID4,
        client_id: ClientId,
        instrument_id: InstrumentId,
        data: Vec<TradeTick>,
        start: Option<UnixNanos>,
        end: Option<UnixNanos>,
        ts_init: UnixNanos,
        params: Option<Params>,
    ) -> Self {
        Self {
            correlation_id,
            client_id,
            instrument_id,
            data,
            start,
            end,
            ts_init,
            params,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FundingRatesResponse {
    pub correlation_id: UUID4,
    pub client_id: ClientId,
    pub instrument_id: InstrumentId,
    pub data: Vec<FundingRateUpdate>,
    pub start: Option<UnixNanos>,
    pub end: Option<UnixNanos>,
    pub ts_init: UnixNanos,
    pub params: Option<Params>,
}

impl FundingRatesResponse {
    /// Converts to a dyn Any trait object for messaging.
    pub fn as_any(&self) -> &dyn Any {
        self
    }

    /// Creates a new [`FundingRatesResponse`] instance.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        correlation_id: UUID4,
        client_id: ClientId,
        instrument_id: InstrumentId,
        data: Vec<FundingRateUpdate>,
        start: Option<UnixNanos>,
        end: Option<UnixNanos>,
        ts_init: UnixNanos,
        params: Option<Params>,
    ) -> Self {
        Self {
            correlation_id,
            client_id,
            instrument_id,
            data,
            start,
            end,
            ts_init,
            params,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OptionChainReferencePriceResponse {
    pub correlation_id: UUID4,
    pub client_id: ClientId,
    pub series_id: OptionSeriesId,
    pub price: Option<Price>,
    pub ts_init: UnixNanos,
    pub params: Option<Params>,
}

impl OptionChainReferencePriceResponse {
    /// Creates a new [`OptionChainReferencePriceResponse`] instance.
    pub fn new(
        correlation_id: UUID4,
        client_id: ClientId,
        series_id: OptionSeriesId,
        price: Option<Price>,
        ts_init: UnixNanos,
        params: Option<Params>,
    ) -> Self {
        Self {
            correlation_id,
            client_id,
            series_id,
            price,
            ts_init,
            params,
        }
    }
}

/// Engine-authored historical request admission or deadline failure.
///
/// This carries original Native request metadata, not a bar payload or reconstructed source.
/// The requested client hint remains in `request`; `client_id` is only an actually resolved
/// source, and can be absent. The process-local request scope is never serialized.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoricalBarsRequestFailure {
    /// Original request intent, including its UUID, bounds, limit and unmodified params.
    pub request: RequestBars,
    /// Actually resolved Native source, if admission reached source resolution.
    pub client_id: Option<ClientId>,
    /// Explicit request failure diagnostic, without a fabricated source-validation result.
    pub error: String,
    /// Standard identities from a successfully parsed original aggregation plan.
    ///
    /// Malformed plans remain in original params; an empty list does not mean successful history.
    pub aggregate_bar_types: Vec<BarType>,
    /// Engine time at failure creation.
    pub ts_init: UnixNanos,
}

/// Actual Native aggregate output for one explicitly requested standard target `BarType`.
///
/// Empty data retains its target identity; consumers must not infer ownership from a first bar.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HistoricalBarsBatch {
    /// Standard target identity frozen from the original Native aggregation plan.
    pub bar_type: BarType,
    /// Actual Native aggregator emissions, including a valid empty result.
    pub data: Vec<Bar>,
}

/// Outcome of opt-in historical source validation and request-local Native aggregation.
///
/// The enclosing [`BarsResponse`] carries the original request, client, source and UTC coverage.
/// This is constructed by the data engine, never trusted from client-echoed response metadata.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum HistoricalBarsOutcome {
    /// The complete original raw source and actual request-local aggregation passed validation.
    Validated {
        /// One named batch per original target, in original target order.
        aggregates: Vec<HistoricalBarsBatch>,
    },
    /// The original source, query or aggregation could not be admitted.
    Failed {
        /// Explicit failure diagnostic; no partial source or target bars are returned.
        error: String,
        /// Requested target identities retained even when no target bar exists.
        aggregate_bar_types: Vec<BarType>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BarsResponse {
    pub correlation_id: UUID4,
    pub client_id: ClientId,
    pub bar_type: BarType,
    pub data: Vec<Bar>,
    pub ts_init: UnixNanos,
    pub start: Option<UnixNanos>,
    pub end: Option<UnixNanos>,
    pub params: Option<Params>,
    /// Engine-authored outcome, absent for the default partial-history profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub historical_outcome: Option<HistoricalBarsOutcome>,
}

impl BarsResponse {
    /// Converts to a dyn Any trait object for messaging.
    pub fn as_any(&self) -> &dyn Any {
        self
    }

    /// Creates a new [`BarsResponse`] instance.
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        correlation_id: UUID4,
        client_id: ClientId,
        bar_type: BarType,
        data: Vec<Bar>,
        start: Option<UnixNanos>,
        end: Option<UnixNanos>,
        ts_init: UnixNanos,
        params: Option<Params>,
    ) -> Self {
        Self {
            correlation_id,
            client_id,
            bar_type,
            data,
            ts_init,
            start,
            end,
            params,
            historical_outcome: None,
        }
    }
}
