import { ROUTING_RULES } from "@/data/data-sources-reference";
import { SectionHeading } from "./section-heading";
import { Route } from "lucide-react";

export function RoutingRules() {
  return (
    <section className="mb-10">
      <SectionHeading id="routing" icon={Route} title="Query Routing & Source Selection" />
      <p className="mb-4 text-sm text-muted-foreground">
        The agentic orchestrator (gpt-5-nano) classifies each query into a route and selects
        1-4 optimal data sources. Profile-driven enrichments and keyword triggers refine
        the selection. Falls back to keyword heuristics if the LLM call fails.
      </p>
      <div className="grid gap-3 md:grid-cols-2">
        {ROUTING_RULES.map((rule) => (
          <div
            key={rule.number}
            className="rounded-lg border border-border bg-card/90 p-4"
          >
            <div className="mb-2 flex items-start gap-3">
              <span className="inline-flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-primary/20 font-mono text-xs font-bold text-primary">
                {rule.number}
              </span>
              <h4 className="text-sm font-semibold text-foreground">{rule.title}</h4>
            </div>
            <p className="pl-9 text-sm text-muted-foreground">{rule.description}</p>
          </div>
        ))}
      </div>
    </section>
  );
}
