-- Predictive optimization mirror tables (PostgreSQL, demo schema).
-- Idempotent by design.

CREATE SCHEMA IF NOT EXISTS demo;

CREATE TABLE IF NOT EXISTS demo.delay_predictions_current (
  id BIGSERIAL PRIMARY KEY,
  as_of_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  model_variant TEXT NOT NULL CHECK (model_variant IN ('baseline', 'optimized')),
  model_version TEXT,
  flight_leg_id TEXT NOT NULL,
  flight_number TEXT,
  origin TEXT,
  dest TEXT,
  std_utc TIMESTAMPTZ,
  risk_a15 DOUBLE PRECISION,
  expected_delay_minutes DOUBLE PRECISION,
  prediction_interval_low DOUBLE PRECISION,
  prediction_interval_high DOUBLE PRECISION,
  top_drivers JSONB NOT NULL DEFAULT '[]'::jsonb,
  data_freshness TEXT,
  degraded_sources JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_delay_predictions_model_std
  ON demo.delay_predictions_current (model_variant, std_utc DESC);

CREATE INDEX IF NOT EXISTS idx_delay_predictions_asof
  ON demo.delay_predictions_current (as_of_utc DESC);

CREATE TABLE IF NOT EXISTS demo.delay_model_metrics_latest (
  id BIGSERIAL PRIMARY KEY,
  as_of_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  sample_window TEXT,
  baseline_auroc DOUBLE PRECISION,
  baseline_brier DOUBLE PRECISION,
  baseline_mae DOUBLE PRECISION,
  optimized_auroc DOUBLE PRECISION,
  optimized_brier DOUBLE PRECISION,
  optimized_mae DOUBLE PRECISION,
  auroc_delta DOUBLE PRECISION,
  brier_delta DOUBLE PRECISION,
  mae_delta DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_delay_metrics_asof
  ON demo.delay_model_metrics_latest (as_of_utc DESC);

CREATE TABLE IF NOT EXISTS demo.delay_action_recommendations_current (
  id BIGSERIAL PRIMARY KEY,
  as_of_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  model_variant TEXT NOT NULL CHECK (model_variant IN ('baseline', 'optimized')),
  flight_leg_id TEXT NOT NULL,
  flight_number TEXT,
  action_rank INTEGER,
  action_code TEXT,
  action_label TEXT,
  expected_delta_minutes DOUBLE PRECISION,
  feasibility_status TEXT,
  confidence_band TEXT,
  constraint_notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_delay_actions_model_rank
  ON demo.delay_action_recommendations_current (model_variant, action_rank);

CREATE TABLE IF NOT EXISTS demo.delay_decision_trace (
  id BIGSERIAL PRIMARY KEY,
  as_of_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  model_variant TEXT,
  model_version TEXT,
  decision_policy_version TEXT,
  constraint_version TEXT,
  objective_version TEXT,
  flight_leg_id TEXT,
  selected_action_code TEXT,
  feasibility_status TEXT,
  override_reason TEXT,
  approved_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_delay_decision_trace_asof
  ON demo.delay_decision_trace (as_of_utc DESC);

