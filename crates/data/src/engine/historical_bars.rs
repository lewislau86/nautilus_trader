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

//! Validates closed 1m historical sources against original, bounded request intent.
//!
//! Validation reads a complete page without changing bars, cache or aggregators. Response headers
//! and parameters cannot redefine the original source, coverage or record bound. Partial pipeline
//! legs can be checked structurally, but require a complete parent check before downstream effects.

use std::{cell::RefCell, rc::Rc};

use ahash::AHashSet;
use nautilus_common::{
    messages::data::{
        BarsResponse, DataCommand, DataResponse, HistoricalBarsBatch, HistoricalBarsOutcome,
        HistoricalBarsRequestFailure, RequestBars, RequestCommand, RequestScope,
    },
    msgbus::{self, MessagingSwitchboard},
    timer::{TimeEvent, TimeEventCallback},
};
use nautilus_core::{
    Params, UUID4, UnixNanos,
    datetime::{
        NANOSECONDS_IN_MICROSECOND, NANOSECONDS_IN_MILLISECOND, NANOSECONDS_IN_MINUTE,
        try_datetime_to_unix_nanos,
    },
};
use nautilus_model::{
    data::{Bar, BarType},
    enums::{BarAggregation, PriceType},
    identifiers::ClientId,
};

use super::{DataEngine, bar::bar_aggregator_key, requests::RequestBarAggregation};

const HISTORICAL_BAR_TIMEOUT_PARAMETER: &str = "historical_bars_timeout_ms";
const HISTORICAL_BAR_TIMEOUT_TIMER: &str = "historical-bars-request-timeout";
const HISTORICAL_BAR_DEFAULT_TIMEOUT_MS: u64 = 30_000;

/// Original admitted Native intent and Engine-clock budget, shared by its entire source subtree.
#[derive(Clone, Debug)]
pub(super) struct HistoricalBarDeadline {
    request: RequestBars,
    client_id: ClientId,
    expires_at: UnixNanos,
    timeout_ms: u64,
}

/// Bounded actual Native output, private to one original request and its frozen target plan.
#[derive(Debug)]
pub(super) struct HistoricalBarOutputs {
    batches: Vec<HistoricalBarsBatch>,
    max_per_target: usize,
    error: Option<String>,
    closed: bool,
}

impl HistoricalBarOutputs {
    fn new(state: Option<&RequestBarAggregation>, max_per_target: usize) -> anyhow::Result<Self> {
        let mut batches = Vec::new();

        if let Some(state) = state {
            for target in &state.bar_types {
                let bar_type = target.standard();
                anyhow::ensure!(
                    !batches
                        .iter()
                        .any(|batch: &HistoricalBarsBatch| batch.bar_type == bar_type),
                    "Historical aggregate targets must have unique standard BarTypes"
                );
                batches.push(HistoricalBarsBatch {
                    bar_type,
                    data: Vec::new(),
                });
            }
        }
        Ok(Self {
            batches,
            max_per_target,
            error: None,
            closed: false,
        })
    }

    /// Records an actual aggregator emission without cache effects or OHLCV reconstruction.
    pub(super) fn record(&mut self, bar: Bar) -> bool {
        if self.closed || self.error.is_some() {
            return false;
        }
        let Some(batch) = self
            .batches
            .iter_mut()
            .find(|batch| batch.bar_type == bar.bar_type)
        else {
            self.error = Some("Native aggregate output has an unrequested BarType".to_string());
            return false;
        };

        if batch.data.len() >= self.max_per_target {
            self.error =
                Some("Native aggregate output exceeds original source count bound".to_string());
            return false;
        }
        batch.data.push(bar);
        true
    }

    fn finish(&mut self) -> anyhow::Result<Vec<HistoricalBarsBatch>> {
        self.closed = true;

        if let Some(e) = &self.error {
            anyhow::bail!("{e}");
        }
        Ok(std::mem::take(&mut self.batches))
    }
}

/// Immutable source identity and coverage captured before dispatching the original request.
#[derive(Clone, Debug)]
pub(super) struct HistoricalBarSource {
    pub(super) request_id: UUID4,
    pub(super) client_id: ClientId,
    pub(super) bar_type: BarType,
    pub(super) start_ns: UnixNanos,
    pub(super) end_ns: UnixNanos,
    parent_request_id: Option<UUID4>,
    params: Option<Params>,
    scope: Option<RequestScope>,
    first_close_ns: u64,
    last_close_ns: u64,
    pub(super) expected_count: usize,
    output_limit: usize,
    aggregation: Option<RequestBarAggregation>,
    pub(super) outputs: Rc<RefCell<HistoricalBarOutputs>>,
}

impl HistoricalBarSource {
    /// Captures exact UTC source coverage, bounded by the original request's explicit limit.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsupported source, missing bounds, an empty grid, invalid dates or
    /// a requested limit which cannot contain the complete source window.
    pub(super) fn new(request: &RequestBars, client_id: ClientId) -> anyhow::Result<Self> {
        Self::from_request(request, client_id, false)
    }

    fn for_child(request: &RequestBars, client_id: ClientId) -> anyhow::Result<Self> {
        Self::from_request(request, client_id, true)
    }

    fn from_request(
        request: &RequestBars,
        client_id: ClientId,
        allow_empty: bool,
    ) -> anyhow::Result<Self> {
        let spec = request.bar_type.spec();
        anyhow::ensure!(
            request.bar_type.is_standard()
                && request.bar_type.is_externally_aggregated()
                && spec.step.get() == 1
                && spec.aggregation == BarAggregation::Minute
                && spec.price_type == PriceType::Last,
            "Historical source validation requires standard 1-MINUTE-LAST-EXTERNAL bars"
        );

        let start_ns = try_datetime_to_unix_nanos(
            request
                .start
                .ok_or_else(|| anyhow::anyhow!("Missing historical source start"))?,
        )?;
        let end_ns = try_datetime_to_unix_nanos(
            request
                .end
                .ok_or_else(|| anyhow::anyhow!("Missing historical source end"))?,
        )?;
        anyhow::ensure!(start_ns <= end_ns, "Historical source start exceeds end");
        let limit = request
            .limit
            .ok_or_else(|| anyhow::anyhow!("Missing historical source limit"))?;

        // Source ts_event uses exchange closeTime, one millisecond before the UTC minute boundary
        let shifted_start = start_ns
            .as_u64()
            .checked_add(NANOSECONDS_IN_MILLISECOND)
            .ok_or_else(|| anyhow::anyhow!("Historical source start overflows"))?;
        let first_close_ns = shifted_start
            .div_ceil(NANOSECONDS_IN_MINUTE)
            .checked_mul(NANOSECONDS_IN_MINUTE)
            .and_then(|boundary| boundary.checked_sub(NANOSECONDS_IN_MILLISECOND))
            .ok_or_else(|| anyhow::anyhow!("Historical source first close overflows"))?;
        let shifted_end = end_ns
            .as_u64()
            .checked_add(NANOSECONDS_IN_MILLISECOND)
            .ok_or_else(|| anyhow::anyhow!("Historical source end overflows"))?;
        let last_close_ns = (shifted_end / NANOSECONDS_IN_MINUTE)
            .checked_mul(NANOSECONDS_IN_MINUTE)
            .and_then(|boundary| boundary.checked_sub(NANOSECONDS_IN_MILLISECOND))
            .unwrap_or(0);
        anyhow::ensure!(
            allow_empty || first_close_ns <= last_close_ns,
            "Historical source contains no closed minute"
        );
        let expected_count = if first_close_ns <= last_close_ns {
            usize::try_from((last_close_ns - first_close_ns) / NANOSECONDS_IN_MINUTE)?
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("Historical source count overflows"))?
        } else {
            0
        };
        anyhow::ensure!(
            expected_count <= limit.get(),
            "Historical source coverage exceeds request limit"
        );

        Ok(Self {
            request_id: request.request_id,
            client_id,
            bar_type: request.bar_type,
            start_ns,
            end_ns,
            parent_request_id: None,
            params: request.params.clone(),
            scope: request.scope.clone(),
            aggregation: None,
            output_limit: limit.get(),
            outputs: Rc::new(RefCell::new(HistoricalBarOutputs::new(None, limit.get())?)),
            first_close_ns,
            last_close_ns,
            expected_count,
        })
    }

    /// Checks an entire raw response before any downstream mutation.
    ///
    /// `complete` is false only for trusted pipeline legs which will be staged and checked again
    /// against their complete parent. Echoed response bounds and counts are deliberately ignored.
    ///
    /// # Errors
    ///
    /// Returns an error for wrong source identity, excess/missing records, invalid OHLCV, non-close
    /// timestamps, out-of-window records, duplicates, disorder or incomplete parent coverage.
    pub(super) fn validate(&self, response: &BarsResponse, complete: bool) -> anyhow::Result<()> {
        anyhow::ensure!(
            response.correlation_id == self.request_id,
            "Historical source request ID mismatch"
        );
        anyhow::ensure!(
            response.client_id == self.client_id,
            "Historical source client ID mismatch"
        );
        anyhow::ensure!(
            response.bar_type == self.bar_type,
            "Historical source BarType mismatch"
        );
        anyhow::ensure!(
            response.data.len() <= self.expected_count,
            "Historical source exceeds original coverage"
        );

        if complete {
            anyhow::ensure!(
                response.data.len() == self.expected_count,
                "Historical source coverage is incomplete"
            );
        }

        let mut previous_close = None;

        for (index, bar) in response.data.iter().enumerate() {
            anyhow::ensure!(
                bar.bar_type == self.bar_type,
                "Historical source row BarType mismatch at {index}"
            );
            validate_ohlcv(bar, index)?;
            let close_ns = canonical_close_ns(bar.ts_event)?;
            anyhow::ensure!(
                close_ns >= self.first_close_ns && close_ns <= self.last_close_ns,
                "Historical source row is outside original coverage at {index}"
            );

            if let Some(previous) = previous_close {
                anyhow::ensure!(
                    close_ns > previous,
                    "Historical source is duplicated or out of order at {index}"
                );
            }

            if complete {
                let offset = u64::try_from(index)?
                    .checked_mul(NANOSECONDS_IN_MINUTE)
                    .ok_or_else(|| anyhow::anyhow!("Historical source row offset overflows"))?;
                anyhow::ensure!(
                    close_ns == self.first_close_ns + offset,
                    "Historical source has a gap at {index}"
                );
            }
            previous_close = Some(close_ns);
        }
        Ok(())
    }
}

impl DataEngine {
    /// Freezes the original timeout budget and arms one shared Native alert before transport.
    ///
    /// The alert retains no requester or source payload. Reusing one named callback bounds clock
    /// callback storage; old queued alerts inspect current original deadlines instead of UUID echoes.
    fn capture_historical_bar_deadline(
        &mut self,
        request: &RequestBars,
        client_id: ClientId,
    ) -> anyhow::Result<()> {
        let timeout_ms = match request
            .params
            .as_ref()
            .and_then(|params| params.get(HISTORICAL_BAR_TIMEOUT_PARAMETER))
        {
            Some(value) => value.as_u64().filter(|value| *value > 0).ok_or_else(|| {
                anyhow::anyhow!("{HISTORICAL_BAR_TIMEOUT_PARAMETER} must be a positive integer")
            })?,
            None => HISTORICAL_BAR_DEFAULT_TIMEOUT_MS,
        };
        let timeout_ns = timeout_ms
            .checked_mul(NANOSECONDS_IN_MILLISECOND)
            .ok_or_else(|| {
                anyhow::anyhow!("{HISTORICAL_BAR_TIMEOUT_PARAMETER} overflows nanoseconds")
            })?;
        let expires_at = self
            .clock
            .borrow()
            .timestamp_ns()
            .as_u64()
            .checked_add(timeout_ns)
            .ok_or_else(|| {
                anyhow::anyhow!("{HISTORICAL_BAR_TIMEOUT_PARAMETER} overflows deadline")
            })?;
        anyhow::ensure!(
            msgbus::has_data_command_endpoint(MessagingSwitchboard::data_engine_queue_execute()),
            "Historical request deadlines require the Native data command queue endpoint"
        );
        self.historical_bar_deadlines.insert(
            request.request_id,
            HistoricalBarDeadline {
                request: request.clone(),
                client_id,
                expires_at: UnixNanos::from(expires_at),
                timeout_ms,
            },
        );

        if let Err(e) = self.maintain_historical_bar_timeout() {
            self.retire_historical_bar_deadline(&request.request_id);
            return Err(e);
        }
        Ok(())
    }

    fn maintain_historical_bar_timeout(&self) -> anyhow::Result<()> {
        let Some(deadline) = self
            .historical_bar_deadlines
            .values()
            .map(|state| state.expires_at)
            .min()
        else {
            self.clock
                .borrow_mut()
                .cancel_timer(HISTORICAL_BAR_TIMEOUT_TIMER);
            return Ok(());
        };
        let clock = self.clock.borrow();
        if clock
            .next_time_ns(HISTORICAL_BAR_TIMEOUT_TIMER)
            .is_some_and(|next| next > clock.timestamp_ns() && next <= deadline)
        {
            return Ok(());
        }
        drop(clock);
        let callback_fn: Rc<dyn Fn(TimeEvent)> = Rc::new(move |_| {
            msgbus::send_data_command(
                MessagingSwitchboard::data_engine_queue_execute(),
                DataCommand::ExpireHistoricalBars,
            );
        });
        self.clock.borrow_mut().set_time_alert_ns(
            HISTORICAL_BAR_TIMEOUT_TIMER,
            deadline,
            Some(TimeEventCallback::from(callback_fn)),
            Some(true),
        )
    }

    /// Removes only this original budget, retaining any earlier wake-up for other pending roots.
    pub(super) fn retire_historical_bar_deadline(&mut self, request_id: &UUID4) {
        self.historical_bar_deadlines.remove(request_id);
        if self.historical_bar_deadlines.is_empty() {
            self.clock
                .borrow_mut()
                .cancel_timer(HISTORICAL_BAR_TIMEOUT_TIMER);
        }
    }

    fn historical_bar_deadline_has_elapsed(&self, request_id: UUID4) -> bool {
        let mut next = Some(request_id);
        let mut visited = AHashSet::new();
        while let Some(id) = next {
            if !visited.insert(id) {
                return false;
            }

            if let Some(deadline) = self.historical_bar_deadlines.get(&id) {
                return self.clock.borrow().timestamp_ns() >= deadline.expires_at;
            }
            next = self.historical_bar_parent_id(id);
        }
        false
    }

    /// Retires expired/inactive source trees before original metadata callbacks, without Bar effects.
    pub(super) fn expire_historical_bar_requests(&mut self) {
        if !self.config.validate_historical_bars {
            return;
        }
        let now = self.clock.borrow().timestamp_ns();
        let expired: Vec<_> = self
            .historical_bar_deadlines
            .values()
            .filter(|state| {
                state.expires_at <= now
                    || state
                        .request
                        .scope
                        .as_ref()
                        .is_some_and(|scope| !scope.is_active())
            })
            .cloned()
            .collect();

        for state in &expired {
            self.discard_historical_bar_sources(state.request.request_id);
        }

        for state in expired {
            self.send_historical_bar_request_failure(
                state.request,
                Some(state.client_id),
                format!(
                    "Historical bar request timed out after {} ms",
                    state.timeout_ms
                ),
            );
        }

        if let Err(e) = self.maintain_historical_bar_timeout() {
            // A lost shared clock alert cannot leave opt-in requesters waiting indefinitely
            let pending: Vec<_> = self.historical_bar_deadlines.values().cloned().collect();
            for state in &pending {
                self.discard_historical_bar_sources(state.request.request_id);
            }

            for state in pending {
                self.send_historical_bar_request_failure(
                    state.request,
                    Some(state.client_id),
                    format!("Unable to schedule historical request deadline: {e}"),
                );
            }
        }
    }

    /// Freezes the original target plan without starting shared or request-local aggregators.
    pub(super) fn freeze_historical_bar_aggregation(
        &mut self,
        request_id: UUID4,
        aggregation: Option<RequestBarAggregation>,
    ) -> anyhow::Result<()> {
        let source = self
            .historical_bar_sources
            .get_mut(&request_id)
            .ok_or_else(|| anyhow::anyhow!("Historical aggregate plan has no original source"))?;
        source.aggregation = aggregation;
        if let Some(state) = &source.aggregation {
            for target in &state.bar_types {
                let spec = target.spec();
                let finer = (spec.aggregation == BarAggregation::Millisecond
                    && spec.step.get() < 60_000)
                    || (spec.aggregation == BarAggregation::Second && spec.step.get() < 60);
                anyhow::ensure!(
                    !finer,
                    "Historical time aggregate target cannot be finer than the original 1m source"
                );
                let mut current = *target;
                let mut visited = Vec::new();

                loop {
                    anyhow::ensure!(
                        current.is_composite() && !visited.contains(&current.standard()),
                        "Historical aggregate target has no acyclic path to the original source"
                    );
                    visited.push(current.standard());
                    let input = current.composite().standard();
                    if input == source.bar_type {
                        break;
                    }
                    current = state
                        .bar_types
                        .iter()
                        .find(|candidate| candidate.standard() == input)
                        .copied()
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "Historical aggregate target has no path to the original source",
                            )
                        })?;
                }
            }
        }
        source.outputs = Rc::new(RefCell::new(HistoricalBarOutputs::new(
            source.aggregation.as_ref(),
            source.output_limit,
        )?));
        Ok(())
    }

    /// Completes actual Native aggregation before admitting any raw or target cache effects.
    pub(super) fn complete_historical_bar_response(&mut self, response: &mut DataResponse) -> bool {
        if !self.config.validate_historical_bars || !matches!(response, DataResponse::Bars(_)) {
            return true;
        }
        let request_id = *response.correlation_id();
        if self.historical_bar_deadline_has_elapsed(request_id) {
            self.expire_historical_bar_requests();
            return false;
        }
        let Some(source) = self.historical_bar_sources.get(&request_id).cloned() else {
            return false;
        };

        if let Some(state) = &source.aggregation
            && let Err(e) = self.prepare_validated_historical_bar_aggregators(request_id, state)
        {
            self.reject_historical_bar_source(request_id, &e.to_string());
            return false;
        }
        self.process_request_bar_aggregation_response(response);
        let aggregates = match source.outputs.borrow_mut().finish() {
            Ok(aggregates) => aggregates,
            Err(e) => {
                self.reject_historical_bar_source(request_id, &e.to_string());
                return false;
            }
        };

        if let DataResponse::Bars(bars) = response {
            bars.historical_outcome = Some(HistoricalBarsOutcome::Validated { aggregates });
        }
        true
    }

    /// Starts a fresh Native aggregation state while retaining existing weak subscription handles.
    fn prepare_validated_historical_bar_aggregators(
        &mut self,
        request_id: UUID4,
        state: &RequestBarAggregation,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.can_start_request_bar_aggregators(request_id, state),
            "Cannot request aggregated bars: one of the aggregators in `bar_types` is already running"
        );
        let aggregator_request_id = state.aggregator_request_id(request_id);
        let mut replacements = Vec::new();

        for bar_type in &state.bar_types {
            let key = bar_aggregator_key(*bar_type, aggregator_request_id);
            if let Some(aggregator) = self.bar_aggregators.get(&key).cloned() {
                let instrument = self
                    .cache
                    .borrow()
                    .instrument(&bar_type.instrument_id())
                    .cloned()
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Cannot start bar aggregation: no instrument found for {}",
                            bar_type.instrument_id(),
                        )
                    })?;
                let replacement = self.create_bar_aggregator(
                    &instrument,
                    *bar_type,
                    state.skip_first_non_full_bar,
                );
                replacements.push((aggregator, replacement));
            }
        }

        // Keep the Rc allocation: existing live subscriptions hold weak references to this slot,
        // replace only inactive state, after all preflight checks and full source validation pass.
        for (aggregator, replacement) in replacements {
            let mut aggregator = aggregator.borrow_mut();
            aggregator.stop();
            *aggregator = replacement;
        }
        self.prepare_request_bar_aggregators_from_state(request_id, state)
    }

    pub(super) fn capture_historical_bar_source(
        &mut self,
        request: &RequestCommand,
        client_id: ClientId,
    ) -> anyhow::Result<()> {
        if !self.config.validate_historical_bars {
            return Ok(());
        }
        let RequestCommand::Bars(request) = request else {
            return Ok(());
        };
        let parent_request_id = self
            .request_pipeline_parent_request_id
            .get(&request.request_id)
            .or_else(|| {
                self.time_range_pipeline_parent_request_id
                    .get(&request.request_id)
            })
            .copied();
        let mut source = if parent_request_id.is_some() {
            HistoricalBarSource::for_child(request, client_id)?
        } else {
            HistoricalBarSource::new(request, client_id)?
        };
        source.parent_request_id = parent_request_id;
        if let Some(parent_id) = source.parent_request_id {
            let parent = self.historical_bar_sources.get(&parent_id).ok_or_else(|| {
                anyhow::anyhow!("Historical source pipeline has no original parent")
            })?;
            source.scope.clone_from(&parent.scope);
        }
        anyhow::ensure!(
            source.scope.as_ref().is_none_or(RequestScope::is_active),
            "Historical bar requester session is inactive"
        );
        anyhow::ensure!(
            !self
                .historical_bar_sources
                .contains_key(&request.request_id),
            "Historical source request ID is already active"
        );

        if parent_request_id.is_none() {
            self.capture_historical_bar_deadline(request, client_id)?;
        }
        self.historical_bar_sources
            .insert(request.request_id, source);
        Ok(())
    }

    /// Applies the opt-in source guard before response trimming, cache updates or aggregation.
    pub(super) fn validate_historical_bar_response(&mut self, response: &mut DataResponse) -> bool {
        // Engine admission failures bypass client response ingestion and cannot be supplied by clients
        if matches!(response, DataResponse::BarsRequestFailed(_)) {
            log::error!("Rejecting client-supplied historical request failure");
            return false;
        }

        if !self.config.validate_historical_bars {
            if let DataResponse::Bars(bars) = response {
                bars.historical_outcome = None;
            }
            return true;
        }
        let request_id = *response.correlation_id();
        let Some(source) = self.historical_bar_sources.get(&request_id).cloned() else {
            if matches!(response, DataResponse::Bars(_)) {
                log::error!(
                    "Rejecting historical bars without an active original request {request_id}"
                );
                return false;
            }
            return true;
        };

        if source
            .scope
            .as_ref()
            .is_some_and(|scope| !scope.is_active())
        {
            if let Some(original) = self.original_historical_bar_source(request_id) {
                self.discard_historical_bar_sources(original.request_id);
            }
            return false;
        }

        if self.historical_bar_deadline_has_elapsed(request_id) {
            self.expire_historical_bar_requests();
            return false;
        }
        let validation = match response {
            DataResponse::Bars(bars) => source.validate(bars, source.parent_request_id.is_none()),
            _ => Err(anyhow::anyhow!(
                "Historical bar source response has wrong data variant"
            )),
        };

        if let Err(e) = validation {
            self.reject_historical_bar_source(request_id, &e.to_string());
            return false;
        }

        if let DataResponse::Bars(bars) = response {
            bars.start = Some(source.start_ns);
            bars.end = Some(source.end_ns);
            bars.params = source.params;
            bars.historical_outcome = None;
        }
        true
    }

    pub(super) fn historical_bar_pipeline_has_capacity(
        &mut self,
        parent_id: UUID4,
        response: &DataResponse,
    ) -> bool {
        let Some(source) = self.historical_bar_sources.get(&parent_id) else {
            return true;
        };
        let expected_count = source.expected_count;
        let received = self
            .request_pipeline_responses
            .get(&parent_id)
            .and_then(|responses| {
                responses.iter().try_fold(0usize, |count, response| {
                    count.checked_add(response.record_count().unwrap_or(0))
                })
            });

        if !received.is_some_and(|received| {
            response.record_count().unwrap_or(0) <= expected_count.saturating_sub(received)
        }) {
            self.reject_historical_bar_source(
                parent_id,
                "Historical pipeline exceeds original coverage",
            );
            return false;
        }
        true
    }

    /// Checks the undeduplicated source union before Native pipeline rebuilding can hide repeats.
    pub(super) fn validate_historical_bar_pipeline_legs(
        &mut self,
        parent_id: UUID4,
        legs: &[DataResponse],
    ) -> bool {
        let Some(source) = self.historical_bar_sources.get(&parent_id).cloned() else {
            return true;
        };
        let mut bars = Vec::new();

        for leg in legs {
            let DataResponse::Bars(response) = leg else {
                self.reject_historical_bar_source(
                    parent_id,
                    "Historical pipeline has wrong source variant",
                );
                return false;
            };

            if response.data.len() > source.expected_count.saturating_sub(bars.len()) {
                self.reject_historical_bar_source(
                    parent_id,
                    "Historical pipeline exceeds original coverage",
                );
                return false;
            }
            bars.extend_from_slice(&response.data);
        }
        bars.sort_unstable_by_key(|bar| bar.ts_event);
        let response = BarsResponse::new(
            parent_id,
            source.client_id,
            source.bar_type,
            bars,
            Some(source.start_ns),
            Some(source.end_ns),
            self.clock.borrow().timestamp_ns(),
            source.params.clone(),
        );

        if let Err(e) = source.validate(&response, source.parent_request_id.is_none()) {
            self.reject_historical_bar_source(parent_id, &e.to_string());
            return false;
        }
        true
    }

    fn original_historical_bar_source(&self, request_id: UUID4) -> Option<HistoricalBarSource> {
        let mut source = self.historical_bar_sources.get(&request_id)?;
        while let Some(parent_id) = source.parent_request_id {
            source = self.historical_bar_sources.get(&parent_id)?;
        }
        Some(source.clone())
    }

    fn historical_bar_parent_id(&self, request_id: UUID4) -> Option<UUID4> {
        self.historical_bar_sources
            .get(&request_id)
            .and_then(|source| source.parent_request_id)
            .or_else(|| {
                self.request_pipeline_parent_request_id
                    .get(&request_id)
                    .copied()
            })
            .or_else(|| {
                self.time_range_pipeline_parent_request_id
                    .get(&request_id)
                    .copied()
            })
    }

    fn historical_bar_source_is_within(&self, request_id: UUID4, parent_id: UUID4) -> bool {
        let mut next_id = Some(request_id);
        let mut visited = AHashSet::new();

        while let Some(request_id) = next_id {
            if request_id == parent_id {
                return true;
            }

            if !visited.insert(request_id) {
                return false;
            }
            next_id = self.historical_bar_parent_id(request_id);
        }
        false
    }

    pub(super) fn discard_historical_bar_sources(&mut self, parent_id: UUID4) {
        let mut request_ids: AHashSet<_> = self
            .historical_bar_sources
            .keys()
            .copied()
            .filter(|request_id| self.historical_bar_source_is_within(*request_id, parent_id))
            .collect();

        if self.config.validate_historical_bars {
            // Frozen pipeline intent exists before source admission and must not outlive its failure
            request_ids.extend(
                self.request_pipeline_parent_request
                    .keys()
                    .chain(self.time_range_pipeline_requests.keys())
                    .copied()
                    .filter(|request_id| {
                        self.historical_bar_source_is_within(*request_id, parent_id)
                    }),
            );
            request_ids.insert(parent_id);
        }

        for request_id in request_ids {
            self.retire_historical_bar_deadline(&request_id);
            self.historical_bar_sources.remove(&request_id);
            self.cleanup_request_bar_aggregators(&request_id);
            self.request_pipeline_parent_request.remove(&request_id);
            self.request_pipeline_n_components.remove(&request_id);
            self.request_pipeline_responses.remove(&request_id);
            self.request_pipeline_parent_request_id
                .retain(|leg, parent| *leg != request_id && *parent != request_id);
            self.time_range_pipeline_requests.remove(&request_id);
            self.time_range_pipeline_parent_request_id
                .retain(|child, parent| *child != request_id && *parent != request_id);
        }
    }

    pub(super) fn discard_all_historical_bar_sources(&mut self) {
        let mut parents: AHashSet<_> = self
            .historical_bar_sources
            .values()
            .filter(|source| source.parent_request_id.is_none())
            .map(|source| source.request_id)
            .collect();
        parents.extend(self.historical_bar_deadlines.keys().copied());
        for parent in parents {
            self.discard_historical_bar_sources(parent);
        }
    }

    /// Completes an admission error which has no captured source, preserving original Native intent.
    ///
    /// Pipeline children fail the captured or frozen original parent without borrowing a child source.
    /// Invalidated scopes suppress delivery; duplicates are rejected before entering this method.
    pub(super) fn reject_unadmitted_historical_bar_request(
        &mut self,
        request: &RequestCommand,
        client_id: Option<ClientId>,
        error: &str,
    ) {
        let RequestCommand::Bars(submitted) = request else {
            return;
        };
        let mut original_id = submitted.request_id;
        let mut visited = AHashSet::new();
        let mut diagnostic = error.to_string();

        while let Some(parent_id) = self.historical_bar_parent_id(original_id) {
            if !visited.insert(original_id) {
                diagnostic.push_str("; historical pipeline ancestry is cyclic");
                original_id = submitted.request_id;
                self.discard_historical_bar_sources(original_id);
                break;
            }
            original_id = parent_id;
        }
        let (request, client_id) = if original_id == submitted.request_id {
            (submitted.clone(), client_id)
        } else {
            if self.original_historical_bar_source(original_id).is_some() {
                self.reject_historical_bar_source(original_id, error);
                return;
            }

            if let Some(RequestCommand::Bars(parent)) = self
                .request_pipeline_parent_request
                .get(&original_id)
                .cloned()
            {
                self.discard_historical_bar_sources(original_id);
                (parent, None)
            } else {
                // Only the submitted Native intent is known; no foreign completion can be invented
                diagnostic.push_str("; original historical pipeline intent missing");
                self.discard_historical_bar_sources(submitted.request_id);
                (submitted.clone(), client_id)
            }
        };
        self.send_historical_bar_request_failure(request, client_id, diagnostic);
    }

    fn send_historical_bar_request_failure(
        &self,
        request: RequestBars,
        client_id: Option<ClientId>,
        diagnostic: String,
    ) {
        if request
            .scope
            .as_ref()
            .is_some_and(|scope| !scope.is_active())
        {
            return;
        }
        let aggregate_bar_types =
            super::requests::request_bar_aggregation_from_params(request.params.as_ref())
                .ok()
                .flatten()
                .map(|state| state.bar_types.iter().map(BarType::standard).collect())
                .unwrap_or_default();
        let request_id = request.request_id;
        let failure = HistoricalBarsRequestFailure {
            request,
            client_id,
            error: diagnostic,
            aggregate_bar_types,
            ts_init: self.clock.borrow().timestamp_ns(),
        };
        log::error!(
            "Historical request failed for {request_id}: {}",
            failure.error
        );
        msgbus::send_response(
            &request_id,
            &DataResponse::BarsRequestFailed(Box::new(failure)),
        );
    }

    pub(super) fn reject_historical_bar_source(&mut self, request_id: UUID4, error: &str) {
        let Some(source) = self.original_historical_bar_source(request_id) else {
            return;
        };
        let parent_id = source.request_id;
        self.discard_historical_bar_sources(parent_id);

        if source
            .scope
            .as_ref()
            .is_some_and(|scope| !scope.is_active())
        {
            return;
        }
        let mut params = source.params.unwrap_or_default();
        params.insert(
            "historical_bar_source_error".to_string(),
            serde_json::json!(error),
        );
        let mut bars = BarsResponse::new(
            parent_id,
            source.client_id,
            source.bar_type,
            Vec::new(),
            Some(source.start_ns),
            Some(source.end_ns),
            self.clock.borrow().timestamp_ns(),
            Some(params),
        );
        bars.historical_outcome = Some(HistoricalBarsOutcome::Failed {
            error: error.to_string(),
            aggregate_bar_types: source
                .aggregation
                .as_ref()
                .map(|state| state.bar_types.iter().map(BarType::standard).collect())
                .unwrap_or_default(),
        });
        let response = DataResponse::Bars(bars);
        log::error!("Historical source validation failed for request {parent_id}: {error}");
        msgbus::send_response(&parent_id, &response);
    }
}

fn canonical_close_ns(timestamp: UnixNanos) -> anyhow::Result<u64> {
    let shifted = timestamp
        .as_u64()
        .checked_add(NANOSECONDS_IN_MILLISECOND)
        .ok_or_else(|| anyhow::anyhow!("Historical source timestamp overflows"))?;
    let remainder = shifted % NANOSECONDS_IN_MINUTE;
    let boundary = if remainder <= NANOSECONDS_IN_MICROSECOND {
        shifted - remainder
    } else if NANOSECONDS_IN_MINUTE - remainder <= NANOSECONDS_IN_MICROSECOND {
        shifted
            .checked_add(NANOSECONDS_IN_MINUTE - remainder)
            .ok_or_else(|| anyhow::anyhow!("Historical source close boundary overflows"))?
    } else {
        anyhow::bail!("Historical source timestamp is not a closed-minute phase");
    };
    boundary
        .checked_sub(NANOSECONDS_IN_MILLISECOND)
        .ok_or_else(|| anyhow::anyhow!("Historical source close precedes epoch"))
}

fn validate_ohlcv(bar: &Bar, index: usize) -> anyhow::Result<()> {
    anyhow::ensure!(
        [bar.open, bar.high, bar.low, bar.close]
            .iter()
            .all(|price| price.is_positive() && !price.is_undefined() && !price.is_error())
            && !bar.volume.is_undefined(),
        "Historical source has invalid price or volume at {index}"
    );
    anyhow::ensure!(
        bar.high >= bar.open
            && bar.high >= bar.low
            && bar.high >= bar.close
            && bar.low <= bar.open
            && bar.low <= bar.close,
        "Historical source has invalid OHLC bounds at {index}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use nautilus_core::Params;
    use nautilus_model::types::{Price, Quantity, quantity::QUANTITY_UNDEF};
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    const START_NS: u64 = 1_735_689_600_000_000_000;

    fn request() -> RequestBars {
        RequestBars::new(
            BarType::from("BTCUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL"),
            Some(UnixNanos::from(START_NS - NANOSECONDS_IN_MINUTE).to_datetime_utc()),
            Some(
                UnixNanos::from(START_NS + 6 * NANOSECONDS_IN_MINUTE - NANOSECONDS_IN_MILLISECOND)
                    .to_datetime_utc(),
            ),
            NonZeroUsize::new(7),
            Some(ClientId::from("BINANCE")),
            UUID4::new(),
            UnixNanos::from(START_NS),
            None,
        )
    }

    fn response(request: &RequestBars) -> BarsResponse {
        let data = (0..7)
            .map(|index| {
                let timestamp = UnixNanos::from(
                    START_NS + index * NANOSECONDS_IN_MINUTE - NANOSECONDS_IN_MILLISECOND,
                );
                Bar::new(
                    request.bar_type,
                    Price::from("100.00"),
                    Price::from("101.00"),
                    Price::from("99.00"),
                    Price::from("100.50"),
                    Quantity::from("1.000"),
                    timestamp,
                    timestamp,
                )
            })
            .collect();
        BarsResponse::new(
            request.request_id,
            ClientId::from("BINANCE"),
            request.bar_type,
            data,
            None,
            None,
            request.ts_init,
            None,
        )
    }

    #[rstest]
    fn test_historical_source_complete_page_does_not_mutate_input() {
        let request = request();
        let source = HistoricalBarSource::new(&request, ClientId::from("BINANCE")).unwrap();
        let response = response(&request);
        let original = response.data.clone();
        source.validate(&response, true).unwrap();
        assert_eq!(response.data, original);
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(3)]
    #[case(6)]
    fn test_historical_source_missing_edge_or_middle_rejects(#[case] missing: usize) {
        let request = request();
        let source = HistoricalBarSource::new(&request, ClientId::from("BINANCE")).unwrap();
        let mut response = response(&request);
        response.data.remove(missing);
        assert!(source.validate(&response, true).is_err());
    }

    #[rstest]
    fn test_historical_source_response_echo_cannot_shrink_original_window() {
        let request = request();
        let source = HistoricalBarSource::new(&request, ClientId::from("BINANCE")).unwrap();
        let mut response = response(&request);
        response.data.remove(3);
        response.start = Some(response.data[0].ts_event);
        response.end = Some(response.data[1].ts_event);
        let mut params = Params::new();
        params.insert("data_count".to_string(), json!(response.data.len()));
        response.params = Some(params);
        assert!(source.validate(&response, true).is_err());
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    fn test_historical_source_rejects_wrong_identity(#[case] mismatch: u8) {
        let request = request();
        let source = HistoricalBarSource::new(&request, ClientId::from("BINANCE")).unwrap();
        let mut response = response(&request);
        match mismatch {
            0 => response.correlation_id = UUID4::new(),
            1 => response.client_id = ClientId::from("OTHER"),
            2 => response.bar_type = BarType::from("ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL"),
            _ => {
                response.data[3].bar_type =
                    BarType::from("ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL");
            }
        }
        assert!(source.validate(&response, true).is_err());
    }

    #[rstest]
    #[case(-1_000, true)]
    #[case(0, true)]
    #[case(128, true)]
    #[case(1_000, true)]
    #[case(-1_001, false)]
    #[case(1_001, false)]
    #[case(1_000_000, false)]
    fn test_historical_source_close_phase_tolerance(#[case] offset: i64, #[case] valid: bool) {
        let request = request();
        let source = HistoricalBarSource::new(&request, ClientId::from("BINANCE")).unwrap();
        let mut response = response(&request);
        for bar in &mut response.data {
            bar.ts_event =
                UnixNanos::from(bar.ts_event.as_u64().checked_add_signed(offset).unwrap());
        }
        assert_eq!(source.validate(&response, true).is_ok(), valid);
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    fn test_historical_source_rejects_bad_tail_before_any_effect(#[case] fault: u8) {
        let request = request();
        let source = HistoricalBarSource::new(&request, ClientId::from("BINANCE")).unwrap();
        let mut response = response(&request);
        match fault {
            0 => response.data[6].ts_event = response.data[5].ts_event,
            1 => response.data.swap(5, 6),
            2 => response.data[6].high = Price::from("90.00"),
            _ => response.data[6].volume = Quantity::from_raw(QUANTITY_UNDEF, 0),
        }
        let original = response.data.clone();
        assert!(source.validate(&response, true).is_err());
        assert_eq!(response.data, original);
    }

    #[rstest]
    fn test_historical_source_partial_leg_requires_complete_parent_recheck() {
        let request = request();
        let source = HistoricalBarSource::new(&request, ClientId::from("BINANCE")).unwrap();
        let mut response = response(&request);
        response.data.remove(3);
        source.validate(&response, false).unwrap();
        assert!(source.validate(&response, true).is_err());
    }

    #[rstest]
    #[case(0)]
    #[case(1)]
    #[case(2)]
    #[case(3)]
    #[case(4)]
    fn test_historical_source_rejects_unbounded_or_unsupported_request(#[case] fault: u8) {
        let mut request = request();
        match fault {
            0 => request.start = None,
            1 => request.end = None,
            2 => request.limit = None,
            3 => request.limit = NonZeroUsize::new(6),
            _ => request.bar_type = BarType::from("BTCUSDT-PERP.BINANCE-5-MINUTE-LAST-EXTERNAL"),
        }
        assert!(HistoricalBarSource::new(&request, ClientId::from("BINANCE")).is_err());
    }
}
