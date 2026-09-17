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

//! Historical delivery regressions through actual Python strategies and the Native data engine.
//!
//! The client supplies in-memory Native transport messages only. These tests do not create a
//! LiveNode, execution client, account connection, external network connection or persistent cache.
//! Owner lifecycle calls use the registered Native component entry, not public Python lifecycle
//! methods. Embedded-class qualification does not qualify an installed extension or live factory.

use std::{cell::RefCell, num::NonZeroUsize, rc::Rc};

use async_trait::async_trait;
use nautilus_common::{
    cache::Cache,
    clients::DataClient,
    clock::{Clock, TestClock},
    component::{component_state, start_component},
    enums::ComponentState,
    messages::data::{
        BarsResponse, DataCommand, DataResponse, RequestBars, SubscribeBars, UnsubscribeBars,
    },
    msgbus::{self, MessageBus, MessagingSwitchboard, TypedIntoHandler, switchboard},
};
use nautilus_core::{Params, UUID4, UnixNanos};
use nautilus_data::{
    client::DataClientAdapter,
    engine::{DataEngine, config::DataEngineConfig},
};
use nautilus_model::{
    data::{Bar, BarType},
    identifiers::{ClientId, StrategyId, TraderId, Venue},
    instruments::{CurrencyPair, InstrumentAny, stubs::audusd_sim},
    stubs::TestDefault,
    types::{Price, Quantity},
};
use nautilus_portfolio::portfolio::Portfolio;
use nautilus_trading::python::strategy::PyStrategy;
use pyo3::{
    ffi::c_str,
    prelude::*,
    types::{PyCFunction, PyDict, PyList, PyModule},
};
use rstest::rstest;
use serde_json::json;

const START_NS: u64 = 1_735_689_600_000_000_000;
const MINUTE_NS: u64 = 60_000_000_000;

/// Records actual Native requests without replacing engine, strategy or lifecycle behavior.
struct HistoricalTransportClient {
    client_id: ClientId,
    venue: Venue,
    requests: Rc<RefCell<Vec<RequestBars>>>,
}

#[async_trait(?Send)]
impl DataClient for HistoricalTransportClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(self.venue)
    }

    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn is_connected(&self) -> bool {
        true
    }

    fn is_disconnected(&self) -> bool {
        false
    }

    fn subscribe_bars(&mut self, _cmd: SubscribeBars) -> anyhow::Result<()> {
        Ok(())
    }

    fn unsubscribe_bars(&mut self, _cmd: &UnsubscribeBars) -> anyhow::Result<()> {
        Ok(())
    }

    fn request_bars(&self, request: RequestBars) -> anyhow::Result<()> {
        self.requests.borrow_mut().push(request);
        Ok(())
    }
}

struct HistoricalStrategyFixture {
    engine: DataEngine,
    cache: Rc<RefCell<Cache>>,
    clock: Rc<RefCell<TestClock>>,
    portfolio: Rc<RefCell<Portfolio>>,
    bus: Rc<RefCell<MessageBus>>,
    queued: Rc<RefCell<Vec<DataCommand>>>,
    requests: Rc<RefCell<Vec<RequestBars>>>,
    request: RequestBars,
    response: BarsResponse,
    target: BarType,
}

fn historical_strategy_fixture(
    instrument: CurrencyPair,
    validate: bool,
) -> HistoricalStrategyFixture {
    let bus =
        MessageBus::new(TraderId::test_default(), UUID4::new(), None, None).register_message_bus();
    let instrument_id = instrument.id;
    let cache = Rc::new(RefCell::new(Cache::default()));
    cache
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(instrument))
        .unwrap();
    let clock = Rc::new(RefCell::new(TestClock::new()));
    clock
        .borrow_mut()
        .set_time(UnixNanos::from(START_NS + 7 * MINUTE_NS));
    let portfolio = Rc::new(RefCell::new(Portfolio::new(
        clock.clone(),
        cache.clone(),
        None,
    )));
    let config = DataEngineConfig::builder()
        .validate_historical_bars(validate)
        .build();
    let mut engine = DataEngine::new(clock.clone(), cache.clone(), Some(config));
    let client_id = ClientId::from("SIM-HISTORY");
    let venue = Venue::from("SIM");
    let requests = Rc::new(RefCell::new(Vec::new()));
    let client = HistoricalTransportClient {
        client_id,
        venue,
        requests: requests.clone(),
    };
    engine.register_client(
        DataClientAdapter::new(client_id, Some(venue), true, true, Box::new(client)),
        None,
    );
    let queued = Rc::new(RefCell::new(Vec::new()));
    let queued_clone = queued.clone();
    msgbus::register_data_command_endpoint(
        MessagingSwitchboard::data_engine_queue_execute(),
        TypedIntoHandler::from(move |command: DataCommand| queued_clone.borrow_mut().push(command)),
    );
    let composite =
        BarType::from(format!("{instrument_id}-5-MINUTE-LAST-INTERNAL@1-MINUTE-EXTERNAL").as_str());
    let source = composite.composite();
    let params: Params = serde_json::from_value(json!({
        "bar_types": [composite.to_string()],
        "update_subscriptions": false,
        "skip_first_non_full_bar": true,
        "disable_build_with_no_updates": true,
    }))
    .unwrap();
    let request = RequestBars::new(
        source,
        Some(UnixNanos::from(START_NS - MINUTE_NS).to_datetime_utc()),
        Some(UnixNanos::from(START_NS + 6 * MINUTE_NS - 1_000_000).to_datetime_utc()),
        NonZeroUsize::new(7),
        Some(client_id),
        UUID4::new(),
        UnixNanos::from(START_NS),
        Some(params.clone()),
    );
    let bars = (0..7)
        .map(|index| {
            let ts = UnixNanos::from(START_NS + index * MINUTE_NS - 1_000_000);
            Bar::new(
                source,
                Price::from("0.65000"),
                Price::from("0.66000"),
                Price::from("0.64000"),
                Price::from("0.65500"),
                Quantity::from(100),
                ts,
                ts,
            )
        })
        .collect();
    let response = BarsResponse::new(
        request.request_id,
        client_id,
        source,
        bars,
        None,
        None,
        request.ts_init,
        Some(params),
    );
    HistoricalStrategyFixture {
        engine,
        cache,
        clock,
        portfolio,
        bus,
        queued,
        requests,
        request,
        response,
        target: composite.standard(),
    }
}

fn historical_request_kwargs<'py>(py: Python<'py>, request: &RequestBars) -> Bound<'py, PyDict> {
    let kwargs = PyDict::new(py);
    kwargs.set_item("start", request.start).unwrap();
    kwargs.set_item("end", request.end).unwrap();
    kwargs.set_item("client_id", request.client_id).unwrap();
    kwargs.set_item("limit", 7).unwrap();
    let params = PyDict::new(py);
    params
        .set_item(
            "bar_types",
            request.params.as_ref().unwrap()["bar_types"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str().unwrap())
                .collect::<Vec<_>>(),
        )
        .unwrap();
    params.set_item("skip_first_non_full_bar", true).unwrap();
    params
        .set_item("disable_build_with_no_updates", true)
        .unwrap();
    params.set_item("update_subscriptions", false).unwrap();
    kwargs.set_item("params", params).unwrap();
    kwargs
}

fn request_history(
    strategy: &Bound<'_, PyAny>,
    request: &RequestBars,
    kwargs: &Bound<'_, PyDict>,
) -> UUID4 {
    strategy
        .call_method("request_bars", (request.bar_type,), Some(kwargs))
        .unwrap()
        .extract::<String>()
        .unwrap()
        .parse()
        .unwrap()
}

fn list_len(object: &Bound<'_, PyAny>, attribute: &str) -> usize {
    object
        .getattr(attribute)
        .unwrap()
        .cast_into::<PyList>()
        .unwrap()
        .len()
}

#[rstest]
#[case::source_indicator(0)]
#[case::target_indicator(1)]
#[case::validated_hook(2)]
#[case::rejected_hook(3)]
#[case::admission_hook(4)]
#[case::timeout_hook(5)]
fn test_historical_delivery_error_actual_python_strategy(
    audusd_sim: CurrencyPair,
    #[case] stage: u8,
    #[values(false, true)] fail_lifecycle: bool,
    #[values(false, true)] ready: bool,
) {
    let HistoricalStrategyFixture {
        mut engine,
        cache,
        clock,
        portfolio,
        bus,
        queued,
        requests,
        request,
        response,
        target,
    } = historical_strategy_fixture(audusd_sim, true);
    Python::initialize();
    Python::attach(|py| {
        let module = PyModule::new(py, "actual_strategy_history_delivery_failure").unwrap();
        module.add_class::<PyStrategy>().unwrap();
        py.run(
            c_str!(
                r#"
class BarIndicator:
    initialized = True
    def __init__(self, fail: bool) -> None:
        self.fail = fail
        self.bars = []
    def handle_bar(self, bar: object) -> None:
        self.bars.append(bar)
        if self.fail:
            raise RuntimeError("Python indicator failure")
class Receiver(Strategy):
    def __init__(self) -> None:
        super().__init__()
        self.stage = 5
        self.fail_lifecycle = False
        self.responses = []
        self.failures = []
        self.raw = []
        self.live = []
        self.faults = 0
        self.disposals = 0
        self.observe_retirement = None
        self.retirements = []
    def on_historical_bars(self, bars: list[object]) -> None:
        self.raw.append(bars)
    def on_historical_bars_response(self, response: object) -> None:
        self.responses.append(response)
        if self.stage in (2, 3):
            raise RuntimeError("Python response hook failure")
    def on_historical_bars_request_failed(self, failure: object) -> None:
        self.failures.append(failure)
        raise RuntimeError("Python admission hook failure")
    def on_bar(self, bar: object) -> None:
        self.live.append(bar)
    def on_fault(self) -> None:
        self.faults += 1
        self.retirements.append(self.observe_retirement())
        if self.fail_lifecycle:
            raise RuntimeError("Python fault hook failure")
    def on_dispose(self) -> None:
        self.disposals += 1
        self.retirements.append(self.observe_retirement())
        if self.fail_lifecycle:
            raise RuntimeError("Python disposal hook failure")
"#
            ),
            Some(&module.dict()),
            Some(&module.dict()),
        )
        .unwrap();
        let receiver = module.getattr("Receiver").unwrap().call0().unwrap();
        let sibling = module.getattr("Receiver").unwrap().call0().unwrap();
        receiver.setattr("stage", stage).unwrap();
        receiver.setattr("fail_lifecycle", fail_lifecycle).unwrap();
        let owner = receiver.extract::<Py<PyStrategy>>().unwrap();
        let sibling_owner = sibling.extract::<Py<PyStrategy>>().unwrap();

        for (wrapper, id, tag) in [
            (&owner, "HISTORY-001", "001"),
            (&sibling_owner, "HISTORY-002", "002"),
        ] {
            let mut strategy = wrapper.borrow_mut(py);
            strategy.set_order_id_tag(tag).unwrap();
            strategy.set_strategy_id(StrategyId::from(id)).unwrap();
            strategy
                .register(
                    TraderId::test_default(),
                    clock.clone(),
                    cache.clone(),
                    portfolio.clone(),
                )
                .unwrap();
            strategy.register_in_global_registries().unwrap();
        }
        let actor_id = owner.borrow(py).strategy_id().inner();
        let sibling_actor_id = sibling_owner.borrow(py).strategy_id().inner();
        assert_ne!(actor_id, sibling_actor_id);
        if !ready {
            start_component(&actor_id).unwrap();
        }
        start_component(&sibling_actor_id).unwrap();
        let source_indicator = module
            .getattr("BarIndicator")
            .unwrap()
            .call1((stage == 0,))
            .unwrap();
        let target_indicator = module
            .getattr("BarIndicator")
            .unwrap()
            .call1((stage == 1,))
            .unwrap();
        receiver
            .call_method1(
                "register_indicator_for_bars",
                (request.bar_type, &source_indicator),
            )
            .unwrap();
        receiver
            .call_method1("register_indicator_for_bars", (target, &target_indicator))
            .unwrap();

        for strategy in [&receiver, &sibling] {
            strategy
                .call_method1("subscribe_bars", (request.bar_type,))
                .unwrap();
        }
        let subscriptions = std::mem::take(&mut *queued.borrow_mut());
        for command in subscriptions {
            engine.execute(command);
        }
        let kwargs = historical_request_kwargs(py, &request);
        if stage == 4 {
            kwargs.del_item("limit").unwrap();
        } else if stage == 5 {
            kwargs
                .get_item("params")
                .unwrap()
                .unwrap()
                .set_item("historical_bars_timeout_ms", 2)
                .unwrap();
        }
        let first_id = request_history(&receiver, &request, &kwargs);
        kwargs.set_item("limit", 7).unwrap();
        if stage == 5 {
            kwargs
                .get_item("params")
                .unwrap()
                .unwrap()
                .set_item("historical_bars_timeout_ms", 100)
                .unwrap();
        }
        let pending_id = request_history(&receiver, &request, &kwargs);
        let sibling_id = request_history(&sibling, &request, &kwargs);

        // Observe the actual Native bus from the author hook, before a lifecycle hook can fail
        let observed_ids = [first_id, pending_id, sibling_id];
        let observer = PyCFunction::new_closure(py, None, None, move |_args, _kwargs| {
            let bus = msgbus::get_message_bus();
            let bus = bus.borrow();
            Ok::<_, PyErr>(
                observed_ids
                    .iter()
                    .map(|id| bus.get_response_handler(id).is_some())
                    .collect::<Vec<_>>(),
            )
        })
        .unwrap();
        receiver.setattr("observe_retirement", observer).unwrap();
        let commands = std::mem::take(&mut *queued.borrow_mut());
        assert_eq!(commands.len(), 3);
        engine.execute(commands[1].clone());
        engine.execute(commands[2].clone());
        let DataCommand::Request(command) = commands[0].clone() else {
            panic!("Python strategies must use the actual Native request route");
        };

        if stage == 4 {
            assert!(engine.execute_request(command).is_err());
        } else {
            engine.execute_request(command).unwrap();

            if stage == 5 {
                let expired = UnixNanos::from(clock.borrow().timestamp_ns().as_u64() + 2_000_000);
                let events = clock.borrow_mut().advance_time(expired, true);
                let handlers = clock.borrow().match_handlers(events);
                for handler in handlers {
                    handler.run();
                }
                assert_eq!(list_len(&receiver, "failures"), 0);
                let expiry = std::mem::take(&mut *queued.borrow_mut());
                assert!(matches!(
                    expiry.as_slice(),
                    [DataCommand::ExpireHistoricalBars]
                ));

                for command in expiry {
                    engine.execute(command);
                }
            } else {
                let mut first_response = response.clone();
                first_response.correlation_id = first_id;
                if stage == 3 {
                    first_response.data.remove(3);
                }
                engine.response(DataResponse::Bars(first_response));
            }
        }
        let expected_state = match (ready, fail_lifecycle) {
            (false, false) | (true, true) => ComponentState::Faulted,
            (false, true) => ComponentState::Faulting,
            (true, false) => ComponentState::Disposed,
        };
        assert_eq!(component_state(&actor_id).unwrap(), expected_state);
        assert_eq!(
            component_state(&sibling_actor_id).unwrap(),
            ComponentState::Running
        );
        assert_eq!(
            receiver
                .getattr("faults")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            usize::from(!ready)
        );
        assert_eq!(
            receiver
                .getattr("disposals")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            usize::from(ready)
        );
        assert_eq!(
            receiver
                .getattr("retirements")
                .unwrap()
                .extract::<Vec<Vec<bool>>>()
                .unwrap(),
            vec![vec![false, false, true]]
        );
        assert!(start_component(&actor_id).is_err());
        assert!(
            receiver
                .call_method("request_bars", (request.bar_type,), Some(&kwargs))
                .is_err()
        );
        assert!(bus.borrow().get_response_handler(&first_id).is_none());
        assert!(bus.borrow().get_response_handler(&pending_id).is_none());
        assert!(bus.borrow().get_response_handler(&sibling_id).is_some());
        assert_eq!(requests.borrow().len(), if stage == 4 { 2 } else { 3 });
        assert!(
            requests
                .borrow()
                .iter()
                .all(|value| value.bar_type == request.bar_type)
        );
        assert_eq!(
            cache.borrow().bar_count(&request.bar_type),
            if stage < 3 { 7 } else { 0 }
        );
        assert_eq!(cache.borrow().bar_count(&target), usize::from(stage < 3));
        let source_snapshot = cache.borrow().bars(&request.bar_type);
        let target_snapshot = cache.borrow().bars(&target);
        let source_count = match stage {
            0 => 1,
            1 | 2 => 7,
            3..=5 => 0,
            _ => panic!("Unknown strategy delivery failure stage"),
        };
        assert_eq!(list_len(&source_indicator, "bars"), source_count);
        assert_eq!(
            list_len(&target_indicator, "bars"),
            usize::from(matches!(stage, 1 | 2))
        );
        assert_eq!(
            list_len(&receiver, "responses"),
            usize::from(matches!(stage, 2 | 3))
        );
        assert_eq!(
            list_len(&receiver, "failures"),
            usize::from(matches!(stage, 4 | 5))
        );

        if stage == 2 || stage == 3 {
            let view = receiver.getattr("responses").unwrap().get_item(0).unwrap();
            assert_eq!(
                view.getattr("correlation_id")
                    .unwrap()
                    .extract::<UUID4>()
                    .unwrap(),
                first_id
            );
            assert_eq!(
                view.getattr("historical_outcome")
                    .unwrap()
                    .getattr("is_validated")
                    .unwrap()
                    .extract::<bool>()
                    .unwrap(),
                stage == 2
            );
            let bars = view
                .call_method0("bars")
                .unwrap()
                .extract::<Vec<Bar>>()
                .unwrap();

            if stage == 2 {
                assert_eq!(bars, response.data);
                let aggregates = view
                    .getattr("historical_outcome")
                    .unwrap()
                    .call_method0("aggregates")
                    .unwrap()
                    .cast_into::<PyList>()
                    .unwrap();
                assert_eq!(aggregates.len(), 1);
                let batch = aggregates.get_item(0).unwrap();
                assert_eq!(
                    batch
                        .getattr("bar_type")
                        .unwrap()
                        .extract::<BarType>()
                        .unwrap(),
                    target
                );
                assert_eq!(
                    Some(
                        batch
                            .call_method0("bars")
                            .unwrap()
                            .extract::<Vec<Bar>>()
                            .unwrap()
                    ),
                    target_snapshot
                );
            } else {
                assert!(bars.is_empty());
            }
        } else if stage == 4 || stage == 5 {
            let view = receiver.getattr("failures").unwrap().get_item(0).unwrap();
            assert_eq!(
                view.getattr("correlation_id")
                    .unwrap()
                    .extract::<UUID4>()
                    .unwrap(),
                first_id
            );

            if stage == 5 {
                assert!(
                    view.getattr("error")
                        .unwrap()
                        .extract::<String>()
                        .unwrap()
                        .contains("timed out")
                );
            }
        }
        let mut late = response.clone();
        late.data[0].close = Price::from("0.65600");
        for id in [first_id, pending_id] {
            late.correlation_id = id;
            engine.response(DataResponse::Bars(late.clone()));
        }
        let cleanup = std::mem::take(&mut *queued.borrow_mut());
        assert_eq!(
            cleanup
                .iter()
                .filter_map(|command| match command {
                    DataCommand::CancelHistoricalBars(id) => Some(*id),
                    _ => None,
                })
                .collect::<Vec<_>>(),
            vec![pending_id]
        );

        for command in cleanup {
            engine.execute(command);
        }

        for id in [first_id, pending_id] {
            late.correlation_id = id;
            engine.response(DataResponse::Bars(late.clone()));
        }
        msgbus::publish_bar(
            switchboard::get_bars_topic(request.bar_type),
            &response.data[0],
        );
        assert_eq!(cache.borrow().bars(&request.bar_type), source_snapshot);
        assert_eq!(cache.borrow().bars(&target), target_snapshot);
        assert_eq!(list_len(&source_indicator, "bars"), source_count);
        assert_eq!(
            list_len(&target_indicator, "bars"),
            usize::from(matches!(stage, 1 | 2))
        );
        assert_eq!(
            list_len(&receiver, "responses"),
            usize::from(matches!(stage, 2 | 3))
        );
        assert_eq!(
            list_len(&receiver, "failures"),
            usize::from(matches!(stage, 4 | 5))
        );
        assert_eq!(list_len(&receiver, "raw"), 0);
        assert_eq!(list_len(&receiver, "live"), 0);
        assert_eq!(list_len(&sibling, "live"), 1);
        let mut sibling_response = response.clone();
        sibling_response.correlation_id = sibling_id;
        engine.response(DataResponse::Bars(sibling_response));
        assert_eq!(
            component_state(&sibling_actor_id).unwrap(),
            ComponentState::Running
        );
        let sibling_view = sibling.getattr("responses").unwrap().get_item(0).unwrap();
        assert_eq!(list_len(&sibling, "responses"), 1);
        assert_eq!(list_len(&sibling, "failures"), 0);
        assert_eq!(list_len(&sibling, "raw"), 0);
        assert_eq!(
            sibling_view
                .call_method0("bars")
                .unwrap()
                .extract::<Vec<Bar>>()
                .unwrap(),
            response.data
        );
        assert_eq!(
            sibling_view
                .getattr("correlation_id")
                .unwrap()
                .extract::<UUID4>()
                .unwrap(),
            sibling_id
        );
        assert!(
            sibling_view
                .getattr("historical_outcome")
                .unwrap()
                .getattr("is_validated")
                .unwrap()
                .extract::<bool>()
                .unwrap()
        );
        assert!(bus.borrow().get_response_handler(&sibling_id).is_none());
        assert_eq!(component_state(&actor_id).unwrap(), expected_state);
        assert_eq!(list_len(&receiver, "retirements"), 1);
    });
}

#[rstest]
#[case::source_indicator(true)]
#[case::raw_hook(false)]
fn test_historical_delivery_error_actual_python_strategy_default_profile(
    audusd_sim: CurrencyPair,
    #[case] fail_indicator: bool,
    #[values(false, true)] ready: bool,
) {
    let HistoricalStrategyFixture {
        mut engine,
        cache,
        clock,
        portfolio,
        bus,
        queued,
        request,
        mut response,
        ..
    } = historical_strategy_fixture(audusd_sim, false);

    // Default partial-history reads remain valid, even with a missing interior minute
    response.data.remove(3);
    let batch_count = response.data.len();
    assert_eq!(batch_count, 6);
    Python::initialize();
    Python::attach(|py| {
        let module = PyModule::new(py, "legacy_strategy_history_delivery_failure").unwrap();
        module.add_class::<PyStrategy>().unwrap();
        py.run(
            c_str!(
                r#"
class BarIndicator:
    initialized = True
    def __init__(self) -> None:
        self.fail = False
        self.bars = []
    def handle_bar(self, bar: object) -> None:
        self.bars.append(bar)
        if self.fail:
            raise RuntimeError("Legacy indicator failure")
class Receiver(Strategy):
    def __init__(self) -> None:
        super().__init__()
        self.raw = []
        self.responses = []
        self.faults = 0
        self.disposals = 0
    def on_historical_bars(self, bars: list[object]) -> None:
        self.raw.append(bars)
        raise RuntimeError("Legacy raw hook failure")
    def on_historical_bars_response(self, response: object) -> None:
        self.responses.append(response)
    def on_fault(self) -> None:
        self.faults += 1
    def on_dispose(self) -> None:
        self.disposals += 1
"#
            ),
            Some(&module.dict()),
            Some(&module.dict()),
        )
        .unwrap();
        let receiver = module.getattr("Receiver").unwrap().call0().unwrap();
        let owner = receiver.extract::<Py<PyStrategy>>().unwrap();
        {
            let mut strategy = owner.borrow_mut(py);
            strategy.set_order_id_tag("001").unwrap();
            strategy
                .set_strategy_id(StrategyId::from("LEGACY-001"))
                .unwrap();
            strategy
                .register(TraderId::test_default(), clock, cache.clone(), portfolio)
                .unwrap();
            strategy.register_in_global_registries().unwrap();
        }
        let actor_id = owner.borrow(py).strategy_id().inner();
        if !ready {
            start_component(&actor_id).unwrap();
        }
        let indicator = module.getattr("BarIndicator").unwrap().call0().unwrap();
        indicator.setattr("fail", fail_indicator).unwrap();
        receiver
            .call_method1(
                "register_indicator_for_bars",
                (request.bar_type, &indicator),
            )
            .unwrap();
        let kwargs = historical_request_kwargs(py, &request);
        let first_id = request_history(&receiver, &request, &kwargs);
        let pending_id = request_history(&receiver, &request, &kwargs);
        let commands = std::mem::take(&mut *queued.borrow_mut());
        assert_eq!(commands.len(), 2);
        for command in commands {
            engine.execute(command);
        }

        for (index, id) in [first_id, pending_id].into_iter().enumerate() {
            let mut reply = response.clone();
            reply.correlation_id = id;
            engine.response(DataResponse::Bars(reply));
            assert!(bus.borrow().get_response_handler(&id).is_none());
            assert_eq!(
                component_state(&actor_id).unwrap(),
                if ready {
                    ComponentState::Ready
                } else {
                    ComponentState::Running
                }
            );
            assert_eq!(
                list_len(&indicator, "bars"),
                (index + 1) * if fail_indicator { 1 } else { batch_count }
            );
            assert_eq!(
                list_len(&receiver, "raw"),
                if fail_indicator { 0 } else { index + 1 }
            );
            assert_eq!(list_len(&receiver, "responses"), 0);
            assert_eq!(
                receiver
                    .getattr("faults")
                    .unwrap()
                    .extract::<usize>()
                    .unwrap(),
                0
            );
            assert_eq!(
                receiver
                    .getattr("disposals")
                    .unwrap()
                    .extract::<usize>()
                    .unwrap(),
                0
            );
            assert!(queued.borrow().is_empty());
            // Baseline Native Cache appends both batches, rather than deduplicating timestamps
            assert_eq!(
                cache.borrow().bar_count(&request.bar_type),
                (index + 1) * batch_count
            );

            if index == 0 {
                assert!(bus.borrow().get_response_handler(&pending_id).is_some());
            }
        }
        let next_id = request_history(&receiver, &request, &kwargs);
        assert!(bus.borrow().get_response_handler(&next_id).is_some());
        assert_eq!(queued.borrow().len(), 1);
        assert_eq!(cache.borrow().bar_count(&request.bar_type), 2 * batch_count);
    });
}
