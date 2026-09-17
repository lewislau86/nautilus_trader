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

//! Read-only Python projections of engine-authored historical bar responses.
//!
//! `HistoricalBarsResponse` retains the original Native response and request UUID. Outcome and
//! batch views share that owned response, so retaining a view after its callback does not depend
//! on a borrowed engine buffer, cache contents or a subsequent request. Python cannot construct
//! these views or mutate their payloads. Bar and identity values remain Native domain objects;
//! collection methods explicitly copy the requested collection without reconstructing OHLCV.
//! These projections perform no source validation, aggregation, I/O or indicator updates.

use std::sync::Arc;

use nautilus_core::{
    UUID4,
    python::{params::params_to_pydict, to_pyruntime_err},
};
use nautilus_model::{
    data::{Bar, BarType},
    identifiers::ClientId,
};
use pyo3::{prelude::*, types::PyDict};

use crate::messages::data::{
    BarsResponse, HistoricalBarsBatch, HistoricalBarsOutcome, HistoricalBarsRequestFailure,
};

#[pyclass(
    name = "HistoricalBarsRequestFailure",
    module = "nautilus_trader.common",
    frozen,
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.common")]
#[derive(Clone, Debug)]
pub struct PyHistoricalBarsRequestFailure {
    failure: Arc<HistoricalBarsRequestFailure>,
}

impl From<HistoricalBarsRequestFailure> for PyHistoricalBarsRequestFailure {
    fn from(failure: HistoricalBarsRequestFailure) -> Self {
        Self {
            failure: Arc::new(failure),
        }
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyHistoricalBarsRequestFailure {
    #[getter]
    fn correlation_id(&self) -> UUID4 {
        self.failure.request.request_id
    }

    #[getter]
    fn client_id(&self) -> Option<ClientId> {
        self.failure.client_id
    }

    #[getter]
    fn requested_client_id(&self) -> Option<ClientId> {
        self.failure.request.client_id
    }

    #[getter]
    fn bar_type(&self) -> BarType {
        self.failure.request.bar_type
    }

    #[getter]
    fn start(&self) -> Option<i128> {
        self.failure
            .request
            .start
            .map(jiff::Timestamp::as_nanosecond)
    }

    #[getter]
    fn end(&self) -> Option<i128> {
        self.failure.request.end.map(jiff::Timestamp::as_nanosecond)
    }

    #[getter]
    fn limit(&self) -> Option<usize> {
        self.failure.request.limit.map(std::num::NonZeroUsize::get)
    }

    #[getter]
    fn request_ts_init(&self) -> u64 {
        self.failure.request.ts_init.as_u64()
    }

    #[getter]
    fn ts_init(&self) -> u64 {
        self.failure.ts_init.as_u64()
    }

    #[getter]
    fn error(&self) -> String {
        self.failure.error.clone()
    }

    fn aggregate_bar_types(&self) -> Vec<BarType> {
        self.failure.aggregate_bar_types.clone()
    }

    fn params(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        self.failure
            .request
            .params
            .as_ref()
            .map(|params| params_to_pydict(py, params))
            .transpose()
    }
}

#[pyclass(
    name = "HistoricalBarsResponse",
    module = "nautilus_trader.common",
    frozen,
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.common")]
#[derive(Clone, Debug)]
pub struct PyHistoricalBarsResponse {
    response: Arc<BarsResponse>,
}

impl From<BarsResponse> for PyHistoricalBarsResponse {
    fn from(response: BarsResponse) -> Self {
        Self {
            response: Arc::new(response),
        }
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyHistoricalBarsResponse {
    #[getter]
    fn correlation_id(&self) -> UUID4 {
        self.response.correlation_id
    }

    #[getter]
    fn client_id(&self) -> ClientId {
        self.response.client_id
    }

    #[getter]
    fn bar_type(&self) -> BarType {
        self.response.bar_type
    }

    #[getter]
    fn start(&self) -> Option<u64> {
        self.response.start.map(|value| value.as_u64())
    }

    #[getter]
    fn end(&self) -> Option<u64> {
        self.response.end.map(|value| value.as_u64())
    }

    #[getter]
    fn ts_init(&self) -> u64 {
        self.response.ts_init.as_u64()
    }

    #[getter]
    fn historical_outcome(&self) -> Option<PyHistoricalBarsOutcome> {
        self.response
            .historical_outcome
            .as_ref()
            .map(|_| PyHistoricalBarsOutcome {
                response: Arc::clone(&self.response),
            })
    }

    fn params(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
        self.response
            .params
            .as_ref()
            .map(|params| params_to_pydict(py, params))
            .transpose()
    }

    #[must_use]
    pub fn bars(&self) -> Vec<Bar> {
        self.response.data.clone()
    }
}

#[pyclass(
    name = "HistoricalBarsOutcome",
    module = "nautilus_trader.common",
    frozen,
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.common")]
#[derive(Clone, Debug)]
pub struct PyHistoricalBarsOutcome {
    response: Arc<BarsResponse>,
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyHistoricalBarsOutcome {
    #[getter]
    fn is_validated(&self) -> bool {
        matches!(
            self.response.historical_outcome,
            Some(HistoricalBarsOutcome::Validated { .. })
        )
    }

    #[getter]
    fn error(&self) -> Option<String> {
        match &self.response.historical_outcome {
            Some(HistoricalBarsOutcome::Failed { error, .. }) => Some(error.clone()),
            _ => None,
        }
    }

    fn aggregate_bar_types(&self) -> Vec<BarType> {
        match &self.response.historical_outcome {
            Some(HistoricalBarsOutcome::Validated { aggregates }) => {
                aggregates.iter().map(|batch| batch.bar_type).collect()
            }
            Some(HistoricalBarsOutcome::Failed {
                aggregate_bar_types,
                ..
            }) => aggregate_bar_types.clone(),
            None => Vec::new(),
        }
    }

    fn aggregates(&self) -> Vec<PyHistoricalBarsBatch> {
        match &self.response.historical_outcome {
            Some(HistoricalBarsOutcome::Validated { aggregates }) => (0..aggregates.len())
                .map(|index| PyHistoricalBarsBatch {
                    response: Arc::clone(&self.response),
                    index,
                })
                .collect(),
            _ => Vec::new(),
        }
    }
}

#[pyclass(
    name = "HistoricalBarsBatch",
    module = "nautilus_trader.common",
    frozen,
    from_py_object
)]
#[pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.common")]
#[derive(Clone, Debug)]
pub struct PyHistoricalBarsBatch {
    response: Arc<BarsResponse>,
    index: usize,
}

impl PyHistoricalBarsBatch {
    fn payload(&self) -> PyResult<&HistoricalBarsBatch> {
        // Views are only created from a validated, immutable response and retain that response
        match &self.response.historical_outcome {
            Some(HistoricalBarsOutcome::Validated { aggregates }) => aggregates.get(self.index),
            _ => None,
        }
        .ok_or_else(|| to_pyruntime_err("Historical bar batch does not belong to its response"))
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl PyHistoricalBarsBatch {
    #[getter]
    fn bar_type(&self) -> PyResult<BarType> {
        Ok(self.payload()?.bar_type)
    }

    fn bars(&self) -> PyResult<Vec<Bar>> {
        Ok(self.payload()?.data.clone())
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::{Params, UnixNanos};
    use nautilus_model::types::{Price, Quantity};
    use pyo3::{
        exceptions::{PyAttributeError, PyTypeError},
        types::{PyList, PyModule},
    };
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    fn response(outcome: Option<HistoricalBarsOutcome>) -> BarsResponse {
        let bar = Bar::new(
            BarType::from("AUD/USD.SIM-1-MINUTE-LAST-EXTERNAL"),
            Price::from("1.12001"),
            Price::from("1.13002"),
            Price::from("1.11003"),
            Price::from("1.12504"),
            Quantity::from("127"),
            UnixNanos::from(u64::MAX - 21),
            UnixNanos::from(u64::MAX - 11),
        );
        let mut response = BarsResponse::new(
            UUID4::new(),
            ClientId::from("SIM"),
            bar.bar_type,
            vec![bar],
            Some(UnixNanos::from(u64::MAX - 101)),
            Some(UnixNanos::from(u64::MAX - 31)),
            UnixNanos::from(u64::MAX - 1),
            Some(Params::from_index_map(indexmap::IndexMap::from([
                ("limit".to_string(), json!(u64::MAX)),
                ("nested".to_string(), json!({"original": true})),
            ]))),
        );
        response.historical_outcome = outcome;
        response
    }

    #[rstest]
    fn test_python_history_common_module_exports_actual_owned_view_types() {
        Python::initialize();
        Python::attach(|py| {
            let module = PyModule::new(py, "common").unwrap();
            crate::python::common(py, &module).unwrap();
            let expected = [
                (
                    "HistoricalBarsResponse",
                    py.get_type::<PyHistoricalBarsResponse>(),
                ),
                (
                    "HistoricalBarsOutcome",
                    py.get_type::<PyHistoricalBarsOutcome>(),
                ),
                (
                    "HistoricalBarsBatch",
                    py.get_type::<PyHistoricalBarsBatch>(),
                ),
                (
                    "HistoricalBarsRequestFailure",
                    py.get_type::<PyHistoricalBarsRequestFailure>(),
                ),
            ];

            for (name, expected_type) in expected {
                let exported = module.getattr(name).unwrap();
                assert!(exported.is(&expected_type));
                assert_eq!(
                    exported
                        .getattr("__module__")
                        .unwrap()
                        .extract::<String>()
                        .unwrap(),
                    "nautilus_trader.common",
                );
                assert_eq!(
                    exported
                        .getattr("__name__")
                        .unwrap()
                        .extract::<String>()
                        .unwrap(),
                    name,
                );
                assert!(
                    exported
                        .call0()
                        .unwrap_err()
                        .is_instance_of::<PyTypeError>(py)
                );
                assert!(!module.hasattr(format!("Py{name}")).unwrap());
            }
        });
    }

    #[rstest]
    fn test_python_history_response_native_fields_and_collection_copies() {
        Python::initialize();
        Python::attach(|py| {
            let response = response(None);
            let expected = response.clone();
            let view = Py::new(py, PyHistoricalBarsResponse::from(response)).unwrap();
            let view = view.bind(py);
            let bars = view.call_method0("bars").unwrap();
            let params = view.call_method0("params").unwrap();

            assert_eq!(
                view.getattr("correlation_id")
                    .unwrap()
                    .extract::<UUID4>()
                    .unwrap(),
                expected.correlation_id,
            );
            assert_eq!(
                view.getattr("client_id")
                    .unwrap()
                    .extract::<ClientId>()
                    .unwrap(),
                expected.client_id,
            );
            assert_eq!(
                view.getattr("bar_type")
                    .unwrap()
                    .extract::<BarType>()
                    .unwrap(),
                expected.bar_type,
            );
            assert_eq!(
                view.getattr("start")
                    .unwrap()
                    .extract::<Option<u64>>()
                    .unwrap(),
                expected.start.map(|value| value.as_u64()),
            );
            assert_eq!(
                view.getattr("end")
                    .unwrap()
                    .extract::<Option<u64>>()
                    .unwrap(),
                expected.end.map(|value| value.as_u64()),
            );
            assert_eq!(
                view.getattr("ts_init").unwrap().extract::<u64>().unwrap(),
                expected.ts_init.as_u64(),
            );
            assert!(view.getattr("historical_outcome").unwrap().is_none());
            assert_eq!(bars.extract::<Vec<Bar>>().unwrap(), expected.data);
            assert_eq!(
                params.get_item("limit").unwrap().extract::<u64>().unwrap(),
                u64::MAX
            );

            bars.call_method0("clear").unwrap();
            params.set_item("limit", 1).unwrap();
            params
                .get_item("nested")
                .unwrap()
                .set_item("original", false)
                .unwrap();

            assert_eq!(
                view.call_method0("bars")
                    .unwrap()
                    .extract::<Vec<Bar>>()
                    .unwrap(),
                expected.data
            );
            let fresh = view.call_method0("params").unwrap();
            assert_eq!(
                fresh.get_item("limit").unwrap().extract::<u64>().unwrap(),
                u64::MAX
            );
            assert!(
                fresh
                    .get_item("nested")
                    .unwrap()
                    .get_item("original")
                    .unwrap()
                    .extract::<bool>()
                    .unwrap()
            );
        });
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn test_python_history_admission_failure_owned_metadata(#[case] resolved: bool) {
        use std::num::NonZeroUsize;

        use crate::messages::data::RequestBars;

        Python::initialize();
        Python::attach(|py| {
            let target = BarType::from("AUD/USD.SIM-5-MINUTE-LAST-INTERNAL");
            let failure = HistoricalBarsRequestFailure {
                request: RequestBars::new(
                    BarType::from("AUD/USD.SIM-1-MINUTE-LAST-EXTERNAL"),
                    Some(jiff::Timestamp::from_nanosecond(-1).unwrap()),
                    None,
                    NonZeroUsize::new(17),
                    Some(ClientId::from("REQUESTED-HINT")),
                    UUID4::new(),
                    UnixNanos::from(u64::MAX - 1),
                    Some(Params::from_index_map(indexmap::IndexMap::from([(
                        "nested".to_string(),
                        json!({"original": true}),
                    )]))),
                ),
                client_id: resolved.then(|| ClientId::from("RESOLVED-SOURCE")),
                error: "invalid original source coverage".to_string(),
                aggregate_bar_types: vec![target],
                ts_init: UnixNanos::from(u64::MAX),
            };
            let expected = failure.clone();
            let view = Py::new(py, PyHistoricalBarsRequestFailure::from(failure)).unwrap();
            let view = view.bind(py);
            assert_eq!(
                view.getattr("correlation_id")
                    .unwrap()
                    .extract::<UUID4>()
                    .unwrap(),
                expected.request.request_id
            );
            assert_eq!(
                view.getattr("client_id")
                    .unwrap()
                    .extract::<Option<ClientId>>()
                    .unwrap(),
                expected.client_id
            );
            assert_eq!(
                view.getattr("requested_client_id")
                    .unwrap()
                    .extract::<ClientId>()
                    .unwrap(),
                expected.request.client_id.unwrap()
            );
            assert_eq!(
                view.getattr("bar_type")
                    .unwrap()
                    .extract::<BarType>()
                    .unwrap(),
                expected.request.bar_type
            );
            assert_eq!(
                view.getattr("start").unwrap().extract::<i128>().unwrap(),
                -1
            );
            assert!(view.getattr("end").unwrap().is_none());
            assert_eq!(
                view.getattr("limit").unwrap().extract::<usize>().unwrap(),
                17
            );
            assert_eq!(
                view.getattr("request_ts_init")
                    .unwrap()
                    .extract::<u64>()
                    .unwrap(),
                u64::MAX - 1
            );
            assert_eq!(
                view.getattr("ts_init").unwrap().extract::<u64>().unwrap(),
                u64::MAX
            );
            assert_eq!(
                view.getattr("error").unwrap().extract::<String>().unwrap(),
                expected.error
            );
            assert!(!view.hasattr("bars").unwrap());
            view.call_method0("aggregate_bar_types")
                .unwrap()
                .call_method0("clear")
                .unwrap();
            assert_eq!(
                view.call_method0("aggregate_bar_types")
                    .unwrap()
                    .extract::<Vec<BarType>>()
                    .unwrap(),
                vec![target]
            );
            view.call_method0("params")
                .unwrap()
                .get_item("nested")
                .unwrap()
                .set_item("original", false)
                .unwrap();
            assert!(
                view.call_method0("params")
                    .unwrap()
                    .get_item("nested")
                    .unwrap()
                    .get_item("original")
                    .unwrap()
                    .extract::<bool>()
                    .unwrap()
            );
            assert!(
                view.setattr("error", "rewritten")
                    .unwrap_err()
                    .is_instance_of::<PyAttributeError>(py)
            );
            assert!(
                py.get_type::<PyHistoricalBarsRequestFailure>()
                    .call0()
                    .unwrap_err()
                    .is_instance_of::<PyTypeError>(py)
            );
            drop(expected);
            assert_eq!(
                view.getattr("start").unwrap().extract::<i128>().unwrap(),
                -1
            );
        });
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn test_python_history_outcome_named_targets_survive_parent_drop(#[case] failed: bool) {
        Python::initialize();
        Python::attach(|py| {
            let target = BarType::from("AUD/USD.SIM-5-MINUTE-LAST-INTERNAL");
            let empty_target = BarType::from("AUD/USD.SIM-10-MINUTE-LAST-INTERNAL");
            let mut aggregate = response(None).data[0];
            aggregate.bar_type = target;
            let outcome = if failed {
                HistoricalBarsOutcome::Failed {
                    error: "incomplete original 1m coverage".to_string(),
                    aggregate_bar_types: vec![target, empty_target],
                }
            } else {
                HistoricalBarsOutcome::Validated {
                    aggregates: vec![
                        HistoricalBarsBatch {
                            bar_type: target,
                            data: vec![aggregate],
                        },
                        HistoricalBarsBatch {
                            bar_type: empty_target,
                            data: Vec::new(),
                        },
                    ],
                }
            };
            let root =
                Py::new(py, PyHistoricalBarsResponse::from(response(Some(outcome)))).unwrap();
            let outcome = root
                .bind(py)
                .getattr("historical_outcome")
                .unwrap()
                .unbind();
            let batches = outcome
                .bind(py)
                .call_method0("aggregates")
                .unwrap()
                .unbind();
            drop(root);

            assert_eq!(
                outcome
                    .bind(py)
                    .getattr("is_validated")
                    .unwrap()
                    .extract::<bool>()
                    .unwrap(),
                !failed,
            );
            assert_eq!(
                outcome
                    .bind(py)
                    .getattr("error")
                    .unwrap()
                    .extract::<Option<String>>()
                    .unwrap(),
                failed.then(|| "incomplete original 1m coverage".to_string()),
            );
            assert_eq!(
                outcome
                    .bind(py)
                    .call_method0("aggregate_bar_types")
                    .unwrap()
                    .extract::<Vec<BarType>>()
                    .unwrap(),
                vec![target, empty_target],
            );
            assert_eq!(
                batches.bind(py).cast::<PyList>().unwrap().len(),
                if failed { 0 } else { 2 }
            );

            if !failed {
                let batch = batches.bind(py).get_item(0).unwrap().unbind();
                let empty = batches.bind(py).get_item(1).unwrap().unbind();
                drop(outcome);
                drop(batches);
                assert_eq!(
                    batch
                        .bind(py)
                        .getattr("bar_type")
                        .unwrap()
                        .extract::<BarType>()
                        .unwrap(),
                    target
                );
                assert_eq!(
                    empty
                        .bind(py)
                        .getattr("bar_type")
                        .unwrap()
                        .extract::<BarType>()
                        .unwrap(),
                    empty_target
                );
                assert_eq!(
                    batch
                        .bind(py)
                        .call_method0("bars")
                        .unwrap()
                        .extract::<Vec<Bar>>()
                        .unwrap(),
                    vec![aggregate]
                );
                assert!(
                    empty
                        .bind(py)
                        .call_method0("bars")
                        .unwrap()
                        .extract::<Vec<Bar>>()
                        .unwrap()
                        .is_empty()
                );
                batch
                    .bind(py)
                    .call_method0("bars")
                    .unwrap()
                    .call_method0("clear")
                    .unwrap();
                assert_eq!(
                    batch
                        .bind(py)
                        .call_method0("bars")
                        .unwrap()
                        .extract::<Vec<Bar>>()
                        .unwrap(),
                    vec![aggregate]
                );
            }
        });
    }

    #[rstest]
    #[case("HistoricalBarsResponse")]
    #[case("HistoricalBarsOutcome")]
    #[case("HistoricalBarsBatch")]
    fn test_python_history_views_not_constructible_and_frozen(#[case] name: &str) {
        Python::initialize();
        Python::attach(|py| {
            let module = PyModule::new(py, "history_views").unwrap();
            super::super::common(py, &module).unwrap();
            let constructor_error = module.getattr(name).unwrap().call0().unwrap_err();
            let target = BarType::from("AUD/USD.SIM-5-MINUTE-LAST-INTERNAL");
            let root = Py::new(
                py,
                PyHistoricalBarsResponse::from(response(Some(HistoricalBarsOutcome::Validated {
                    aggregates: vec![HistoricalBarsBatch {
                        bar_type: target,
                        data: Vec::new(),
                    }],
                }))),
            )
            .unwrap();
            let view = match name {
                "HistoricalBarsResponse" => root.bind(py).as_any().clone(),
                "HistoricalBarsOutcome" => root.bind(py).getattr("historical_outcome").unwrap(),
                _ => root
                    .bind(py)
                    .getattr("historical_outcome")
                    .unwrap()
                    .call_method0("aggregates")
                    .unwrap()
                    .get_item(0)
                    .unwrap(),
            };
            let mutation_error = if name == "HistoricalBarsOutcome" {
                view.setattr("is_validated", false).unwrap_err()
            } else {
                view.setattr("bar_type", target).unwrap_err()
            };

            assert!(constructor_error.is_instance_of::<PyTypeError>(py));
            assert!(mutation_error.is_instance_of::<PyAttributeError>(py));
        });
    }
}
