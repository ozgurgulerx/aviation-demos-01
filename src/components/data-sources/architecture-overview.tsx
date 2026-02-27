"use client";

import { useState, useEffect } from "react";
import { motion } from "framer-motion";
import { SectionHeading } from "./section-heading";
import { Network } from "lucide-react";

interface SourceBox {
  id: string;
  label: string;
  detail: string;
  stat: string;
  colorClass: string;
  borderColor: string;
}

const STRUCTURED: SourceBox[] = [
  { id: "SQL", label: "SQL", detail: "Azure PostgreSQL", stat: "20+ tables", colorClass: "text-blue-400", borderColor: "border-blue-500/40" },
  { id: "KQL", label: "KQL", detail: "Fabric Eventhouse", stat: "ADS-B, SIGMETs", colorClass: "text-teal-400", borderColor: "border-teal-500/40" },
  { id: "FABRIC_SQL", label: "FABRIC_SQL", detail: "Fabric SQL WH", stat: "BTS on-time data", colorClass: "text-amber-400", borderColor: "border-amber-500/40" },
];

const SEMANTIC: SourceBox[] = [
  { id: "VECTOR_OPS", label: "VECTOR_OPS", detail: "idx_ops_narratives", stat: "240K chunks", colorClass: "text-purple-400", borderColor: "border-purple-500/40" },
  { id: "VECTOR_REG", label: "VECTOR_REG", detail: "idx_regulatory", stat: "55+ docs", colorClass: "text-purple-400", borderColor: "border-purple-500/40" },
  { id: "VECTOR_AIRPORT", label: "VECTOR_AIRPORT", detail: "idx_airport_ops_docs", stat: "2K+ docs", colorClass: "text-purple-400", borderColor: "border-purple-500/40" },
];

const DOCUMENT_GRAPH: SourceBox[] = [
  { id: "NOSQL", label: "NOSQL", detail: "Cosmos DB (notams)", stat: "25+ NOTAMs", colorClass: "text-orange-400", borderColor: "border-orange-500/40" },
  { id: "GRAPH", label: "GRAPH", detail: "Fabric / PG fallback", stat: "500K+ edges", colorClass: "text-green-400", borderColor: "border-green-500/40" },
];

function SourceBoxCard({ box, hovered, onHover, onLeave }: {
  box: SourceBox;
  hovered: string | null;
  onHover: (id: string) => void;
  onLeave: () => void;
}) {
  const isHovered = hovered === box.id;
  return (
    <div
      className={`rounded-md border bg-card/80 px-3 py-2 transition-all ${box.borderColor} ${
        isHovered ? "border-opacity-100 shadow-md" : "border-opacity-60"
      }`}
      onMouseEnter={() => onHover(box.id)}
      onMouseLeave={onLeave}
    >
      <div className={`font-mono text-xs font-bold ${box.colorClass}`}>{box.label}</div>
      <div className="text-xs text-muted-foreground">{box.detail}</div>
      <div className="mt-0.5 text-[0.68rem] text-muted-foreground/70">{box.stat}</div>
    </div>
  );
}

export function ArchitectureOverview() {
  const [isHydrated, setIsHydrated] = useState(false);
  const [hovered, setHovered] = useState<string | null>(null);

  useEffect(() => {
    setIsHydrated(true);
  }, []);

  const Wrapper = isHydrated ? motion.div : "div";
  const wrapperProps = isHydrated
    ? { initial: { opacity: 0 }, animate: { opacity: 1 }, transition: { duration: 0.4 } }
    : {};

  return (
    <section className="mb-10">
      <SectionHeading id="architecture" icon={Network} title="Architecture Overview" />
      <Wrapper {...wrapperProps}>
        <div className="rounded-lg border border-border bg-card/60 p-6">
          {/* Router box */}
          <div className="mx-auto mb-6 max-w-md rounded-lg border-2 border-primary/30 bg-primary/5 px-5 py-3 text-center">
            <div className="font-display text-sm font-semibold text-primary">
              Agentic Query Router (gpt-5-nano)
            </div>
            <div className="text-xs text-muted-foreground">
              route: SQL | SEMANTIC | HYBRID &middot; selects 1-4 sources per query
            </div>
          </div>

          {/* Connector line */}
          <div className="mx-auto mb-4 h-6 w-px bg-border" />

          {/* Three columns */}
          <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
            {/* Structured */}
            <div>
              <div className="mb-3 rounded border border-blue-500/20 bg-blue-500/5 px-3 py-1.5 text-center text-xs font-semibold uppercase tracking-wider text-blue-400">
                Structured
              </div>
              <div className="space-y-2">
                {STRUCTURED.map((box) => (
                  <SourceBoxCard
                    key={box.id}
                    box={box}
                    hovered={hovered}
                    onHover={setHovered}
                    onLeave={() => setHovered(null)}
                  />
                ))}
              </div>
            </div>

            {/* Semantic (Vector) */}
            <div>
              <div className="mb-3 rounded border border-purple-500/20 bg-purple-500/5 px-3 py-1.5 text-center text-xs font-semibold uppercase tracking-wider text-purple-400">
                Semantic (Vector)
              </div>
              <div className="space-y-2">
                {SEMANTIC.map((box) => (
                  <SourceBoxCard
                    key={box.id}
                    box={box}
                    hovered={hovered}
                    onHover={setHovered}
                    onLeave={() => setHovered(null)}
                  />
                ))}
              </div>
            </div>

            {/* Document / Graph */}
            <div>
              <div className="mb-3 rounded border border-orange-500/20 bg-orange-500/5 px-3 py-1.5 text-center text-xs font-semibold uppercase tracking-wider text-orange-400">
                Document / Graph
              </div>
              <div className="space-y-2">
                {DOCUMENT_GRAPH.map((box) => (
                  <SourceBoxCard
                    key={box.id}
                    box={box}
                    hovered={hovered}
                    onHover={setHovered}
                    onLeave={() => setHovered(null)}
                  />
                ))}
              </div>
            </div>
          </div>
        </div>
      </Wrapper>
    </section>
  );
}
