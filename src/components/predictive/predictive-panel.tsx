"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { AlertTriangle, Bot, RefreshCw, Sparkles, TrendingUp } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { ToggleGroup } from "@/components/ui/switch";
import type {
  PredictiveActionsResponse,
  PredictiveDecisionMetricsResponse,
  PredictiveDelaysResponse,
  PredictiveMetricsResponse,
} from "@/types";

type PredictiveModel = "baseline" | "optimized";
type PredictiveTab = "predictions" | "actions";

interface PredictivePanelProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  enableActionsTab?: boolean;
}

function formatMetric(value: number | null | undefined, decimals = 3): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "n/a";
  }
  return value.toFixed(decimals);
}

function formatMinutes(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "n/a";
  }
  return `${Math.round(value)}m`;
}

function formatPct(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "n/a";
  }
  return `${Math.round(value * 100)}%`;
}

export function PredictivePanel({
  open,
  onOpenChange,
  enableActionsTab = false,
}: PredictivePanelProps) {
  const [model, setModel] = useState<PredictiveModel>("optimized");
  const [windowHours, setWindowHours] = useState<number>(6);
  const [activeTab, setActiveTab] = useState<PredictiveTab>("predictions");

  const [loading, setLoading] = useState(false);
  const [delays, setDelays] = useState<PredictiveDelaysResponse | null>(null);
  const [metrics, setMetrics] = useState<PredictiveMetricsResponse | null>(null);
  const [actions, setActions] = useState<PredictiveActionsResponse | null>(null);
  const [decisionMetrics, setDecisionMetrics] = useState<PredictiveDecisionMetricsResponse | null>(null);
  const [fetchError, setFetchError] = useState<string | null>(null);

  useEffect(() => {
    if (!enableActionsTab && activeTab === "actions") {
      setActiveTab("predictions");
    }
  }, [enableActionsTab, activeTab]);

  const loadData = useCallback(async () => {
    if (!open) return;
    setLoading(true);
    setFetchError(null);
    try {
      const delaysQuery = new URLSearchParams({
        model,
        windowHours: String(windowHours),
        limit: "50",
      });
      const tasks: Array<Promise<unknown>> = [
        fetch(`/api/predictive/delays?${delaysQuery.toString()}`, { cache: "no-store" }).then((res) =>
          res.json()
        ),
        fetch("/api/predictive/delay-metrics", { cache: "no-store" }).then((res) => res.json()),
      ];
      if (enableActionsTab) {
        const actionsQuery = new URLSearchParams({ model, limit: "75" });
        tasks.push(
          fetch(`/api/predictive/actions?${actionsQuery.toString()}`, { cache: "no-store" }).then((res) =>
            res.json()
          )
        );
        tasks.push(
          fetch("/api/predictive/decision-metrics", { cache: "no-store" }).then((res) => res.json())
        );
      }

      const [delayPayload, metricsPayload, actionsPayload, decisionPayload] = await Promise.all(tasks);
      setDelays((delayPayload || null) as PredictiveDelaysResponse | null);
      setMetrics((metricsPayload || null) as PredictiveMetricsResponse | null);
      if (enableActionsTab) {
        setActions((actionsPayload || null) as PredictiveActionsResponse | null);
        setDecisionMetrics((decisionPayload || null) as PredictiveDecisionMetricsResponse | null);
      } else {
        setActions(null);
        setDecisionMetrics(null);
      }
    } catch (error) {
      setFetchError(error instanceof Error ? error.message : "Unable to load predictive data");
    } finally {
      setLoading(false);
    }
  }, [enableActionsTab, model, open, windowHours]);

  useEffect(() => {
    if (!open) return;
    void loadData();
  }, [open, loadData]);

  const overallStatus = useMemo(() => {
    const statuses = [delays?.status, metrics?.status, actions?.status, decisionMetrics?.status].filter(Boolean);
    if (statuses.includes("disabled")) return "disabled";
    if (statuses.includes("degraded")) return "degraded";
    if (statuses.includes("error")) return "error";
    if (statuses.includes("empty")) return "empty";
    return "ok";
  }, [actions?.status, decisionMetrics?.status, delays?.status, metrics?.status]);

  const statusVariant =
    overallStatus === "ok"
      ? "success"
      : overallStatus === "empty"
        ? "warning"
        : overallStatus === "disabled"
          ? "secondary"
          : "destructive";

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[88vh] max-w-6xl overflow-hidden p-0">
        <DialogHeader className="border-b border-border/70 px-6 py-4">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div>
              <DialogTitle className="flex items-center gap-2">
                <TrendingUp className="h-4 w-4" />
                Predictive Ops
              </DialogTitle>
              <DialogDescription>
                Baseline vs optimized delay predictions and action-oriented optimization guidance.
              </DialogDescription>
            </div>
            <Badge variant={statusVariant}>Status: {overallStatus.toUpperCase()}</Badge>
          </div>
        </DialogHeader>

        <div className="space-y-3 px-6 pt-4">
          <div className="flex flex-wrap items-center gap-2">
            <ToggleGroup
              value={model}
              onValueChange={(value) => setModel(value as PredictiveModel)}
              options={[
                { value: "baseline", label: "Baseline" },
                { value: "optimized", label: "Optimized" },
              ]}
            />

            <ToggleGroup
              value={String(windowHours)}
              onValueChange={(value) => setWindowHours(Number(value))}
              options={[
                { value: "6", label: "Next 6h" },
                { value: "12", label: "Next 12h" },
                { value: "24", label: "Next 24h" },
              ]}
            />

            {enableActionsTab && (
              <ToggleGroup
                value={activeTab}
                onValueChange={(value) => setActiveTab(value as PredictiveTab)}
                options={[
                  { value: "predictions", label: "Predictions" },
                  { value: "actions", label: "Optimized Actions" },
                ]}
              />
            )}

            <Button
              size="sm"
              variant="outline"
              onClick={() => void loadData()}
              className="ml-auto gap-1.5"
              disabled={loading}
            >
              <RefreshCw className={`h-3.5 w-3.5 ${loading ? "animate-spin" : ""}`} />
              Refresh
            </Button>
          </div>

          {(fetchError || overallStatus === "degraded" || overallStatus === "disabled") && (
            <Card className="border-amber-500/35 bg-amber-500/10">
              <CardContent className="flex items-start gap-2 p-3 text-sm text-foreground">
                <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-amber-600" />
                <p>
                  {fetchError ||
                    delays?.message ||
                    metrics?.message ||
                    actions?.message ||
                    decisionMetrics?.message ||
                    "Predictive module is degraded. Existing chat operations remain unaffected."}
                </p>
              </CardContent>
            </Card>
          )}

          <div className="grid gap-3 md:grid-cols-3">
            <MetricCard title="AUROC" baseline={metrics?.baseline?.auroc} optimized={metrics?.optimized?.auroc} />
            <MetricCard title="Brier" baseline={metrics?.baseline?.brier} optimized={metrics?.optimized?.brier} />
            <MetricCard title="MAE (min)" baseline={metrics?.baseline?.mae} optimized={metrics?.optimized?.mae} />
          </div>
        </div>

        <Separator />

        <ScrollArea className="h-[54vh] px-6 py-4">
          {activeTab === "actions" && enableActionsTab ? (
            <ActionsTable actions={actions} decisionMetrics={decisionMetrics} />
          ) : (
            <PredictionsTable delays={delays} />
          )}
        </ScrollArea>
      </DialogContent>
    </Dialog>
  );
}

function MetricCard({
  title,
  baseline,
  optimized,
}: {
  title: string;
  baseline: number | null | undefined;
  optimized: number | null | undefined;
}) {
  const delta = baseline !== null && baseline !== undefined && optimized !== null && optimized !== undefined
    ? optimized - baseline
    : null;
  return (
    <Card className="bg-card/90">
      <CardHeader className="space-y-1 pb-2">
        <CardTitle className="text-sm">{title}</CardTitle>
      </CardHeader>
      <CardContent className="space-y-1 text-sm">
        <div className="flex items-center justify-between">
          <span className="text-muted-foreground">Baseline</span>
          <span className="font-medium">{formatMetric(baseline, 3)}</span>
        </div>
        <div className="flex items-center justify-between">
          <span className="text-muted-foreground">Optimized</span>
          <span className="font-medium">{formatMetric(optimized, 3)}</span>
        </div>
        <div className="flex items-center justify-between text-xs">
          <span className="text-muted-foreground">Delta</span>
          <span className={delta !== null && delta > 0 ? "text-emerald-600" : "text-amber-600"}>
            {delta === null ? "n/a" : `${delta > 0 ? "+" : ""}${delta.toFixed(3)}`}
          </span>
        </div>
      </CardContent>
    </Card>
  );
}

function PredictionsTable({ delays }: { delays: PredictiveDelaysResponse | null }) {
  const rows = delays?.rows || [];
  if (!rows.length) {
    return (
      <div className="rounded-lg border border-border/70 bg-surface-1/40 p-4 text-sm text-muted-foreground">
        No predictive departure rows available for the selected model/window.
      </div>
    );
  }
  return (
    <div className="overflow-x-auto rounded-lg border border-border/70">
      <table className="w-full min-w-[980px] text-left text-sm">
        <thead className="bg-surface-2">
          <tr>
            <th className="px-3 py-2 font-semibold">Flight</th>
            <th className="px-3 py-2 font-semibold">Route</th>
            <th className="px-3 py-2 font-semibold">A15 Risk</th>
            <th className="px-3 py-2 font-semibold">Expected Delay</th>
            <th className="px-3 py-2 font-semibold">Interval</th>
            <th className="px-3 py-2 font-semibold">Top Drivers</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={`${row.flight_leg_id}-${row.flight_number}`} className="border-t border-border/50">
              <td className="px-3 py-2">{row.flight_number || row.flight_leg_id}</td>
              <td className="px-3 py-2">
                {row.origin || "?"} - {row.dest || "?"}
              </td>
              <td className="px-3 py-2">{formatPct(row.risk_a15)}</td>
              <td className="px-3 py-2">{formatMinutes(row.expected_delay_minutes)}</td>
              <td className="px-3 py-2">
                {formatMinutes(row.prediction_interval?.low)} - {formatMinutes(row.prediction_interval?.high)}
              </td>
              <td className="px-3 py-2">
                <div className="flex flex-wrap gap-1.5">
                  {(row.top_drivers || []).slice(0, 3).map((driver) => (
                    <Badge key={driver} variant="outline" className="max-w-[210px] truncate">
                      {driver}
                    </Badge>
                  ))}
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function ActionsTable({
  actions,
  decisionMetrics,
}: {
  actions: PredictiveActionsResponse | null;
  decisionMetrics: PredictiveDecisionMetricsResponse | null;
}) {
  const rows = actions?.actions || [];
  return (
    <div className="space-y-3">
      <div className="grid gap-2 md:grid-cols-4">
        <Badge variant="outline" className="justify-start gap-1.5 px-2 py-1">
          <Sparkles className="h-3.5 w-3.5" />
          Decisions: {decisionMetrics?.metrics?.total_decisions ?? 0}
        </Badge>
        <Badge variant="outline" className="justify-start gap-1.5 px-2 py-1">
          <Bot className="h-3.5 w-3.5" />
          Overrides: {decisionMetrics?.metrics?.override_count ?? 0}
        </Badge>
        <Badge variant="outline" className="justify-start gap-1.5 px-2 py-1">
          Feasible: {decisionMetrics?.metrics?.feasible_count ?? 0}
        </Badge>
        <Badge variant="outline" className="justify-start gap-1.5 px-2 py-1">
          Variants: {decisionMetrics?.metrics?.model_variant_count ?? 0}
        </Badge>
      </div>

      {!rows.length ? (
        <div className="rounded-lg border border-border/70 bg-surface-1/40 p-4 text-sm text-muted-foreground">
          No action recommendations available for the selected model.
        </div>
      ) : (
        <div className="overflow-x-auto rounded-lg border border-border/70">
          <table className="w-full min-w-[980px] text-left text-sm">
            <thead className="bg-surface-2">
              <tr>
                <th className="px-3 py-2 font-semibold">Flight</th>
                <th className="px-3 py-2 font-semibold">Rank</th>
                <th className="px-3 py-2 font-semibold">Action</th>
                <th className="px-3 py-2 font-semibold">Delta</th>
                <th className="px-3 py-2 font-semibold">Feasibility</th>
                <th className="px-3 py-2 font-semibold">Notes</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr
                  key={`${row.flight_leg_id}-${row.flight_number}-${row.action_rank}-${row.action_code}`}
                  className="border-t border-border/50"
                >
                  <td className="px-3 py-2">{row.flight_number || row.flight_leg_id}</td>
                  <td className="px-3 py-2">{row.action_rank ?? "n/a"}</td>
                  <td className="px-3 py-2">{row.action_label || row.action_code || "n/a"}</td>
                  <td className="px-3 py-2">{formatMinutes(row.expected_delta_minutes)}</td>
                  <td className="px-3 py-2">{row.feasibility_status || "n/a"}</td>
                  <td className="px-3 py-2">{row.constraint_notes || "n/a"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

