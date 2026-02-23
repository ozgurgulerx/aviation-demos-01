# Graph Retrieval Stability Improvement Plan

## Goal
Improve reliability of `GRAPH` retrieval so the agentic retrieval layer remains deterministic under transient Fabric issues, while preserving successful context merge and final LLM responses.

## Scope
- Backend graph retrieval logic in `src/unified_retriever.py`
- Fabric preflight health probing (`/api/fabric/preflight` path)
- Runtime metadata emitted in graph rows/error rows for observability

## Baseline Failure Modes
1. Live graph call failures returned hard errors instead of degrading to fallback.
2. Graph calls shared generic KQL DB selection, causing wrong-db failures.
3. No graph-specific timeout/retry controls for transient errors.
4. No circuit breaker, so repeated failures caused repeated slow/erroring calls.
5. Preflight only checked endpoint reachability, not executable graph query health.
6. Graph result rows lacked explicit path/retry metadata to diagnose merge quality.

## Implementation Plan

### Phase 1: Live/Fallback Routing Hardening
- [x] Add automatic fallback to PostgreSQL graph path when live graph fails.
- [x] Preserve fallback behavior when graph endpoint is not configured.
- [x] Keep live path for successful calls.

### Phase 2: Graph-Specific Kusto Scoping
- [x] Add `FABRIC_GRAPH_DATABASE` env support.
- [x] Extend `_kusto_rows(...)` to accept explicit `database` override.
- [x] Route graph Kusto queries to graph DB first, then existing KQL DB fallback.

### Phase 3: Timeout + Retry Controls
- [x] Add graph runtime controls:
  - `GRAPH_TIMEOUT_SECONDS`
  - `GRAPH_MAX_RETRIES`
  - `GRAPH_RETRY_BACKOFF_SECONDS`
- [x] Implement retry loop for both Kusto-backed and HTTP-backed graph endpoints.
- [x] Retry only on retryable errors (timeouts/5xx/429/connection failures).

### Phase 4: Circuit Breaker
- [x] Add in-memory breaker controls:
  - `GRAPH_CIRCUIT_BREAKER_FAIL_THRESHOLD`
  - `GRAPH_CIRCUIT_BREAKER_OPEN_SECONDS`
- [x] Open circuit on consecutive live errors.
- [x] Skip live path while open and route directly to fallback.
- [x] Reset on successful/empty healthy live executions after cooldown.

### Phase 5: Preflight Depth
- [x] Add `fabric_graph_query_probe` check that executes a real graph query.
- [x] Add `graph_circuit_breaker_state` check for operational visibility.
- [x] Include retry count and graph path in preflight output.

### Phase 6: Traceability Metadata
- [x] Add graph row metadata:
  - `graph_path`
  - `fallback_used`
  - `retry_attempts`
- [x] Include live error and circuit details in degraded/failure responses.

## Validation Plan

### Local Validation
1. `python3 -m py_compile src/unified_retriever.py`
2. Run backend unit/integration tests that cover graph paths if available.

### Cloud Validation (AKS)
1. Deploy backend image with these changes.
2. Run comprehensive datastore combination tests (`scripts/19_test_datastore_combinations.py`) with focus on combos containing `GRAPH`.
3. Validate strict mode behavior:
   - `GRAPH` included when requested and healthy.
   - deterministic fallback metadata when live path is degraded.
4. Validate preflight output from `/api/fabric/preflight` includes:
   - `fabric_graph_query_probe`
   - `graph_circuit_breaker_state`

### Pass Criteria
- Graph calls no longer fail hard on transient live errors when fallback is available.
- Response context includes valid graph path metadata for every graph row.
- Preflight catches graph query-runtime failures before demo execution.
- Combination tests with `GRAPH` no longer fail due unstable live-graph path behavior.

## Follow-up (Recommended)
1. Add persistent circuit-breaker state (Redis) if multi-pod consistency is needed.
2. Add per-source latency/error metrics for graph live vs fallback path.
3. Add CI tests that intentionally inject graph timeout/5xx faults.
