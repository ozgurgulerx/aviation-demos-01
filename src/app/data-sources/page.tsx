import type { Metadata } from "next";
import Link from "next/link";
import { ArrowLeft, ShieldCheck } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { getDataAsOfTimestamp } from "@/data/seed";
import { HeroStats } from "@/components/data-sources/hero-stats";
import { ArchitectureOverview } from "@/components/data-sources/architecture-overview";
import { SourceCatalog } from "@/components/data-sources/source-catalog";
import { SourceRelationships } from "@/components/data-sources/source-relationships";
import { PipelineTable } from "@/components/data-sources/pipeline-table";
import { RoutingRules } from "@/components/data-sources/routing-rules";
import { RetrievalComparison } from "@/components/data-sources/retrieval-comparison";

export const metadata: Metadata = {
  title: "Data Sources | AeroLynx Pilot Brief Bot",
  description:
    "8 canonical data sources across 5 storage technologies, unified by an agentic query router with 3 retrieval routes.",
};

const JUMP_NAV = [
  { label: "Architecture", href: "#architecture" },
  { label: "Sources", href: "#sources" },
  { label: "Relationships", href: "#relationships" },
  { label: "Pipeline", href: "#pipeline" },
  { label: "Routing", href: "#routing" },
  { label: "Modes", href: "#modes" },
];

export default function DataSourcesPage() {
  const dataAsOf = getDataAsOfTimestamp();

  return (
    <div className="relative overflow-hidden">
      <div className="pointer-events-none absolute inset-0 flight-grid opacity-20" />
      <div className="relative z-10 mx-auto w-full max-w-7xl px-4 py-8 md:px-6 md:py-10">
        {/* Hero Section */}
        <section className="mb-8">
          <Card className="border-primary/20 bg-card/90">
            <CardHeader className="space-y-3 text-center">
              <div className="flex flex-wrap items-center justify-center gap-2">
                <span className="mission-chip">Data Sources Reference</span>
                <Badge variant="outline">8 Sources</Badge>
                <Badge variant="outline">5 Storage Technologies</Badge>
                <Badge variant="success">Evidence-first</Badge>
              </div>

              <CardTitle className="font-display text-2xl text-brand-gradient md:text-3xl">
                Aviation RAG — Data Sources Reference
              </CardTitle>
              <CardDescription className="mx-auto max-w-3xl text-sm md:text-base">
                8 canonical data sources across 5 storage technologies, unified by an agentic
                query router with 3 retrieval routes (SQL, SEMANTIC, HYBRID) and multi-source
                orchestration selecting 1-4 sources per query.
              </CardDescription>

              {/* Stats */}
              <div className="pt-4">
                <HeroStats />
              </div>
            </CardHeader>
            <CardContent className="flex flex-wrap items-center justify-between gap-3 text-xs text-muted-foreground">
              <div className="flex flex-wrap items-center gap-2">
                <ShieldCheck className="h-3.5 w-3.5 text-emerald-600 dark:text-emerald-400" />
                <span>Data as-of {dataAsOf}</span>
              </div>
              <Button asChild variant="outline" size="sm">
                <Link href="/chat" className="inline-flex items-center gap-2">
                  <ArrowLeft className="h-4 w-4" />
                  Back to Chat
                </Link>
              </Button>
            </CardContent>
          </Card>
        </section>

        {/* Jump Navigation */}
        <nav className="mb-8 flex flex-wrap items-center justify-center gap-2">
          {JUMP_NAV.map((item) => (
            <a
              key={item.href}
              href={item.href}
              className="rounded-full border border-border bg-card/80 px-3 py-1 text-xs font-medium text-muted-foreground transition-colors hover:border-primary/30 hover:text-foreground"
            >
              {item.label}
            </a>
          ))}
        </nav>

        {/* Main Content */}
        <ArchitectureOverview />
        <SourceCatalog />
        <SourceRelationships />
        <PipelineTable />
        <RoutingRules />
        <RetrievalComparison />
      </div>
    </div>
  );
}
