"use client";

import { useState, useEffect } from "react";
import { motion } from "framer-motion";
import { ChevronDown } from "lucide-react";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import {
  BADGE_COLORS,
  type DataSourceReference,
} from "@/data/data-sources-reference";
import { SampleQueryBox } from "./sample-query-box";
import { ExampleRecord } from "./example-record";

const ACCENT_MAP: Record<string, string> = {
  sql: "#5b8def",
  kql: "#3ec9a7",
  vector: "#a78bfa",
  nosql: "#e8944a",
  graph: "#4ade80",
  fabric: "#e8c44a",
};

interface SourceCardProps {
  source: DataSourceReference;
}

export function SourceCard({ source }: SourceCardProps) {
  const [isHydrated, setIsHydrated] = useState(false);
  const [schemaOpen, setSchemaOpen] = useState(false);
  const [exampleOpen, setExampleOpen] = useState(false);

  useEffect(() => {
    setIsHydrated(true);
  }, []);

  const badge = BADGE_COLORS[source.badgeVariant];
  const accent = ACCENT_MAP[source.badgeVariant] ?? "#d4a843";

  const Wrapper = isHydrated ? motion.div : "div";
  const wrapperProps = isHydrated
    ? { initial: { opacity: 0, y: 12 }, animate: { opacity: 1, y: 0 }, transition: { duration: 0.3 } }
    : {};

  return (
    <Wrapper {...wrapperProps}>
      <Card
        className="border-border/80 bg-card/90 transition-colors hover:border-border"
        id={`src-${source.id.toLowerCase().replace(/_/g, "-")}`}
      >
        {/* Header */}
        <CardHeader className="flex flex-row flex-wrap items-center gap-3 space-y-0 border-b border-border pb-4">
          <span
            className={`inline-flex items-center rounded-full border px-2.5 py-0.5 font-mono text-[0.7rem] font-bold uppercase tracking-wider ${badge.bg} ${badge.text} ${badge.border}`}
          >
            {source.id}
          </span>
          <h3 className="min-w-[200px] flex-1 text-lg font-semibold text-foreground">
            {source.title}
          </h3>
          <span className="rounded border border-border bg-background px-2 py-0.5 font-mono text-xs text-muted-foreground">
            {source.storeTag}
          </span>
        </CardHeader>

        <CardContent className="space-y-5 pt-5">
          {/* What Is This */}
          <div>
            <p className="mb-1.5 text-[0.7rem] font-semibold uppercase tracking-widest text-primary/70">
              What is this?
            </p>
            <div className="rounded-md border border-primary/10 bg-primary/5 px-3 py-2.5 text-sm leading-relaxed text-muted-foreground">
              {source.whatIsThis}
            </div>
          </div>

          {/* Schema — collapsible */}
          <div>
            <button
              onClick={() => setSchemaOpen(!schemaOpen)}
              className="flex w-full items-center gap-2 text-left"
            >
              <p className="text-[0.7rem] font-semibold uppercase tracking-widest text-primary/70">
                Schema
              </p>
              <ChevronDown
                className={`h-3.5 w-3.5 text-muted-foreground transition-transform ${
                  schemaOpen ? "rotate-180" : ""
                }`}
              />
              <span className="text-xs text-muted-foreground">
                {source.schemaTables.length} table{source.schemaTables.length > 1 ? "s" : ""}
              </span>
            </button>
            {schemaOpen && (
              <div className="mt-2 space-y-3">
                {source.schemaTables.map((table) => (
                  <div key={table.name}>
                    <div className="mb-1 flex items-center gap-2">
                      <span className="font-mono text-xs font-semibold text-foreground">
                        {table.name}
                      </span>
                      <span className="text-xs text-muted-foreground">
                        — {table.description}
                      </span>
                    </div>
                    <div className="overflow-x-auto">
                      <table className="w-full border-collapse text-xs">
                        <thead>
                          <tr className="bg-surface-2">
                            <th className="border-b border-border px-3 py-1.5 text-left font-semibold text-muted-foreground">
                              Column
                            </th>
                            <th className="border-b border-border px-3 py-1.5 text-left font-semibold text-muted-foreground">
                              Description
                            </th>
                          </tr>
                        </thead>
                        <tbody>
                          {table.columns.map((col) => (
                            <tr key={col.name} className="odd:bg-card even:bg-surface-1/30">
                              <td className="border-b border-border/50 px-3 py-1 font-mono text-foreground">
                                {col.name}
                              </td>
                              <td className="border-b border-border/50 px-3 py-1 text-muted-foreground">
                                {col.description}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Sample Queries */}
          <div>
            <p className="mb-2 text-[0.7rem] font-semibold uppercase tracking-widest text-primary/70">
              Sample Queries
            </p>
            <div className="space-y-2">
              {source.sampleQueries.map((q) => (
                <SampleQueryBox key={q.query} query={q} accentColor={accent} />
              ))}
            </div>
          </div>

          {/* Example Record — collapsible */}
          <div>
            <button
              onClick={() => setExampleOpen(!exampleOpen)}
              className="flex w-full items-center gap-2 text-left"
            >
              <p className="text-[0.7rem] font-semibold uppercase tracking-widest text-primary/70">
                Example Record
              </p>
              <ChevronDown
                className={`h-3.5 w-3.5 text-muted-foreground transition-transform ${
                  exampleOpen ? "rotate-180" : ""
                }`}
              />
            </button>
            {exampleOpen && (
              <div className="mt-2">
                <ExampleRecord record={source.exampleRecord} />
              </div>
            )}
          </div>

          {/* Trigger Keywords */}
          <div>
            <p className="mb-2 text-[0.7rem] font-semibold uppercase tracking-widest text-primary/70">
              Triggers
            </p>
            <div className="flex flex-wrap gap-1.5">
              {source.triggerKeywords.map((kw) => (
                <span
                  key={kw}
                  className="rounded-full border border-border bg-background px-2 py-0.5 font-mono text-[0.68rem] text-muted-foreground"
                >
                  {kw}
                </span>
              ))}
            </div>
            {source.triggerNote && (
              <p className="mt-2 text-xs text-muted-foreground">
                <strong className="text-foreground">Note:</strong> {source.triggerNote}
              </p>
            )}
          </div>

          {/* Meta Grid */}
          <div className="grid grid-cols-2 gap-3">
            {source.meta.map((m) => (
              <div
                key={m.label}
                className="rounded border border-border bg-background p-2.5"
              >
                <div className="text-[0.68rem] uppercase tracking-wider text-muted-foreground">
                  {m.label}
                </div>
                <div className="mt-0.5 text-sm text-foreground">{m.value}</div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </Wrapper>
  );
}
