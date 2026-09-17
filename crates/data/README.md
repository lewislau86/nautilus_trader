# nautilus-data

[![build](https://github.com/nautechsystems/nautilus_trader/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/nautechsystems/nautilus_trader/actions/workflows/build.yml)
[![Documentation](https://img.shields.io/docsrs/nautilus-data)](https://docs.rs/nautilus-data/latest/nautilus_data/)
[![crates.io version](https://img.shields.io/crates/v/nautilus-data.svg)](https://crates.io/crates/nautilus-data)
![license](https://img.shields.io/github/license/nautechsystems/nautilus_trader?color=blue)
[![Discord](https://img.shields.io/badge/Discord-%235865F2.svg?logo=discord&logoColor=white)](https://discord.gg/NautilusTrader)

Data engine and market data processing for [NautilusTrader](https://nautilustrader.io).

The `nautilus-data` crate provides a framework for handling market data ingestion,
processing, and aggregation within the NautilusTrader ecosystem. This includes real-time
data streaming, historical data management, and various aggregation methodologies:

- High-performance data engine for orchestrating data operations.
- Data client infrastructure for connecting to market data providers.
- Bar aggregation machinery supporting tick, volume, value, and time-based aggregation.
- Order book management and delta processing capabilities.
- Subscription management and data request handling.
- Configurable data routing and processing pipelines.

## NautilusTrader

[NautilusTrader](https://nautilustrader.io) is an open-source, production-grade, Rust-native
engine for multi-asset, multi-venue trading systems.

The system spans research, deterministic simulation, and live execution within a single
event-driven architecture, providing research-to-live semantic parity.

## Feature flags

This crate provides feature flags to control source code inclusion during compilation:

- `defi`: Enables DeFi (Decentralized Finance) support.
- `extension-module`: Enables Python extension-module support.
- `high-precision`: Enables
  [high-precision mode](https://nautilustrader.io/docs/nightly/getting_started/installation/#precision-mode)
  to use 128-bit value types.
- `python`: Enables Python bindings from [PyO3](https://pyo3.rs).
- `streaming`: Enables the `nautilus-persistence` dependency for catalog-based data streaming.

## Documentation

### Fork-local historical source validation (in development)

Rust `DataEngineConfig.validate_historical_bars` defaults to `false`, preserving Native partial-history
semantics. Opting in requires standard `1-MINUTE-LAST-EXTERNAL` sources, explicit UTC start/end and a
limit sufficient for the original complete source window. The engine freezes the request UUID,
resolved client, BarType and coverage before dispatch; response bounds and count parameters cannot
redefine them. Prices and volumes use Native fixed-point domain types throughout validation.
The Rust live-node configuration must forward the same explicit flag without enabling it by
default; the live conversion and Python binding must not silently omit it.

Each raw page is checked before trimming, caching or aggregation. Catalog/client legs remain staged;
their undeduplicated union is checked before pipeline rebuilding. Time-range children are bounded by
the original parent coverage and cannot update bar cache or aggregators before the complete parent
passes. Source or query failures discard staged state and send an empty, original-ID `BarsResponse`
with `historical_bar_source_error` in its Native params. Unknown and repeated responses are rejected;
DataEngine stop/reset invalidates active sources and releases staged state. CloseTime-phase rounding
within one microsecond is checked without changing the original Bar timestamps.

Requester cancellation also covers an individual actor while the engine remains active. Native
actor bar requests carry an optional process-local cancellation scope, not in exchange params or
serialized messages. Stop/reset/fault/dispose invalidate the
scope before user hooks and remove the actor's pending response handlers. A queued original or
child request must retain the same scope; the source guard checks it before downstream effects.
An ordinary Native data command then releases the original request's staged subtree, without
canceling sibling actors, removing cache or depending on a late response to arrive.
The strategy stop decision invalidates pending history even when managed market exit defers
the later component-stop hook; live subscriptions and exit processing must remain unchanged.

The opt-in response contract adds an optional typed `BarsResponse.historical_outcome`. A validated
response retains its original request/client/source identity and contains actual Native aggregate
output batches, one explicitly named standard target BarType per batch, including empty batches.
A failed response retains the same original context, contains no bars and explicitly identifies the
failure and requested targets. Client-echoed outcome metadata is never authoritative. The default
profile omits the new field and preserves the existing raw-bar callback and serialized response.
Native request aggregators start only after the entire original source passes validation. Their
actual handlers collect request-local, source-count-bounded outputs without polling global cache or
reimplementing OHLCV. No raw or target bars reach cache before this collection succeeds. This must
also preserve `update_subscriptions=true`, chained aggregation and concurrent original requests;
source failure or cancellation must not alter an unrelated live aggregator. Python binding and
Crab delivery must retain the original UUID even for empty or failed responses.
An admission error before source capture uses a separate Native
`HistoricalBarsRequestFailure`, retaining the original `RequestBars`, diagnostic, parsed target
identities and optional resolved client. A requested client hint is not a resolved source; absence
of a source remains `None`, never an invented client. Native and Python
`on_historical_bars_request_failed()` receive this metadata on the existing response route without
indicator or cache updates. Python failure views are owned and read-only; requested UTC bounds are
signed nanoseconds so malformed pre-epoch intent is not silently clamped. Duplicate active UUIDs
must not consume the first request's handler. Inactive requester scopes suppress delivery, and
client-supplied failure envelopes cannot impersonate engine-authored completion.
Pipeline admission failures use the retained original parent request even when its source has not
been captured; a resolved child source cannot supply the parent's client identity. Nested pipeline
staging is released before failure delivery, without removing unrelated pipelines. Late child or
parent responses cannot revive the failed request.
Missing or cyclic ancestry cannot invent parent intent: the known submitted request receives its
own admission failure, while unknown or unrelated response handlers remain untouched.
The EventStore captures admission failure context as forensic-only metadata. Cache replay must
ignore this payload and must not publish callbacks or create raw or aggregate bars from it.
Opted-in history must also retain one original Engine-clock deadline across all pipeline children.
The original request may set a positive integer `historical_bars_timeout_ms` parameter; absence uses
30 seconds. Admission requires a registered ownership-based Native data command queue endpoint;
otherwise the request fails explicitly instead of accepting history with no working deadline route.
Child requests and response echoes cannot extend that budget. Native Clock alerts must
enqueue ordinary data commands, not call requesters from timer workers. No reply and a late response
whose deadline command has not yet run must both complete the original request once with Native
request-failure metadata, release its staging and reject later replies before cache or aggregation.
This is request completion, not an invented source-validation result or a transport abort. Pending
unrelated requests, completed requests and the default profile must remain unaffected.
Rust `DataActor.on_historical_bars_response()` receives this context after source and target
indicator updates; failed outcomes skip all indicator updates. Its default implementation calls
the legacy raw-bar hook once. Opted-in source/target indicator and history-hook errors must close
the requester's history scope before user lifecycle hooks, retire its remaining requests and
release its subscriptions, including when the lifecycle hook fails. Eligible components use the
existing fault lifecycle; a pre-start Ready component uses existing disposal because Fault is not
a valid Ready transition. The failure must not change validated source facts, invent another
response or roll back completed Cache/indicator effects. Unrelated requesters and the default
raw-list error behavior must remain unchanged. Requester-local retirement is not global Node
fail-stop or activation of queued callbacks.
The target plan must have an acyclic Native composite path to the
original source; unrooted or ambiguous plans fail instead of being presented as successful empty
history. An inactive aggregator reused for subscription handoff receives fresh Native state while
retaining its Rc allocation and weak subscription handles; an active live aggregator is untouched.
The output bound is the original explicit request limit per target, not a client-echoed count;
complete source coverage is still checked against the separate exact UTC grid count. Time targets
finer than the original 1m source are unrepresentable and fail preflight rather than entering
Native empty-window building.
Validated raw bars advance the existing Native historical aggregator with their original event
timestamps, independently of receipt-time `ts_init`. Raw bars retain both original timestamps in
responses and cache. The default profile and live aggregation continue using their existing clock.
The Python contract uses an explicit keyword-only configuration flag and read-only response,
outcome and batch views in the existing common module. Views retain the owned Native response and
domain types, rather than reconstructing data or creating another aggregation model. Default
delivery remains the raw-list hook; opted-in actors and strategies receive original context through
`on_historical_bars_response()`, after Native indicators. Its Python base implementation delegates
once to the legacy raw-list hook; consumers opting in must inspect the typed outcome for failures.

This is a Rust/PyO3 development stage, not a complete live-history API or release candidate. Python
DataEngine/LiveDataEngine configuration, read-only outcome views and Actor/Strategy delivery now
have development regressions through the actual embedded Native classes, including legacy hooks
and indicator-before-callback ordering. The full generator now produces the public owned views,
typed hooks and keyword-only flag with artifact contract regressions. Crab integration, a clean
extension/wheel and Python/live-runtime lifecycle/factory qualification remain required. Admission
failures now have actual Native requester/Engine/bus/owned-view regressions through the existing
owner lifecycle entry. This does not qualify direct Python lifecycle calls or a live factory.
Indicator/history-hook errors now have actual Native requester/Engine/bus and embedded PyDataActor
and PyStrategy requester regressions, including failing lifecycle hooks, Ready disposal,
remaining-request cancellation, late replies and unaffected independent requesters. Strategy cases
use the actual Native Portfolio and checked component-owner lifecycle entry. Author lifecycle hooks
observe retired requester handlers while the independent strategy retains its handler. Default
partial-history reads, log-only errors and Native Cache batch-append semantics remain unchanged.
These embedded tests create no LiveNode or execution client. This is requester-local closure, not
global Node failure handling. Original-deadline completion now has actual Engine/Clock/queue
regressions for no reply, late replies before the expiry command, catalog fan-in and nested time-range
staging. Default budgets use admission-time Engine Clock, and invalid/overflowing budgets or a missing
Native command queue fail admission. One shared alert preserves independent pending roots; retired
alerts after cancellation/stop/reset cannot revive old requests. Embedded PyStrategy tests include
deadline-failure hooks and lifecycle-hook errors. In-memory transport fixtures do not qualify actual
network timeouts or live timer workers. Crab delivery and live factory/lifecycle qualification remain
outstanding. Rust outcome/requester tests do not qualify an installed extension.
Continuous-future validation is unsupported;
the default profile retains that existing route. Do not enable this stage for Crab field acceptance.

See [the docs](https://docs.rs/nautilus-data) for more detailed usage.

## License

The source code for NautilusTrader is available on GitHub under the [GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).

---

NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

Use of this software is subject to the [Disclaimer](https://nautilustrader.io/legal/disclaimer/).

<img src="https://github.com/nautechsystems/nautilus_trader/raw/develop/assets/nautilus-logo-white.png" alt="logo" width="300" height="auto"/>

© 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
