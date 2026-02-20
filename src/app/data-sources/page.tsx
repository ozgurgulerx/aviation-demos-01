import type { Metadata } from "next";
import Link from "next/link";
import {
  ArrowLeft,
  BookOpenText,
  Database,
  Network,
  Radar,
  ShieldCheck,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import { getDataAsOfTimestamp } from "@/data/seed";
import {
  CONTEXT_TO_DATASTORE_MAPPING,
  DATA_INVENTORY,
  DATA_SHAPE_DETAILS,
  GLOSSARY_ITEMS,
  RETRIEVAL_FLOW_STEPS,
} from "@/data/data-sources-content";

export const metadata: Metadata = {
  title: "Data Sources | AeroLynx Pilot Brief Bot",
  description:
    "Detailed inventory of aviation data sources, datastore mapping, and retrieval roles used in the demo.",
};

export default function DataSourcesPage() {
  const dataAsOf = getDataAsOfTimestamp();

  return (
    <div className="relative overflow-hidden">
      <div className="pointer-events-none absolute inset-0 flight-grid opacity-20" />
      <div className="relative z-10 mx-auto w-full max-w-7xl px-4 py-8 md:px-6 md:py-10">
        <section className="mb-8">
          <Card className="border-primary/20 bg-card/90">
            <CardHeader className="space-y-3">
              <div className="flex flex-wrap items-center gap-2">
                <span className="mission-chip">Data and Retrieval Coverage</span>
                <Badge variant="outline">PPT-aligned</Badge>
                <Badge variant="outline">Repo-verified</Badge>
                <Badge variant="success">Evidence-first</Badge>
              </div>

              <CardTitle className="font-display text-2xl text-brand-gradient md:text-3xl">
                What Data This Demo Uses and Why
              </CardTitle>
              <CardDescription className="max-w-4xl text-sm md:text-base">
                This page explains each source used in the aviation retrieval system, the
                operational purpose of that data, where it is stored, and how it contributes
                to citation-backed pilot briefs.
              </CardDescription>
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

        <section className="mb-8">
          <div className="mb-4 flex items-center gap-2">
            <Database className="h-4 w-4 text-primary" />
            <h2 className="font-display text-xl font-semibold">Data Inventory and Purpose</h2>
          </div>
          <Card>
            <CardContent className="p-0">
              <div className="overflow-x-auto">
                <table className="w-full min-w-[920px] border-collapse text-sm">
                  <thead>
                    <tr className="bg-surface-2 text-left">
                      <th className="border-b border-border px-4 py-3 font-semibold">Source</th>
                      <th className="border-b border-border px-4 py-3 font-semibold">Data Type</th>
                      <th className="border-b border-border px-4 py-3 font-semibold">
                        Why It Matters for Airline Ops
                      </th>
                      <th className="border-b border-border px-4 py-3 font-semibold">
                        Primary Datastore(s)
                      </th>
                      <th className="border-b border-border px-4 py-3 font-semibold">
                        Repo Artifact(s)
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {DATA_INVENTORY.map((item) => (
                      <tr key={item.id} className="align-top odd:bg-card even:bg-surface-1/30">
                        <td className="border-b border-border px-4 py-3 font-medium text-foreground">
                          {item.source}
                        </td>
                        <td className="border-b border-border px-4 py-3 text-muted-foreground">
                          {item.dataType}
                        </td>
                        <td className="border-b border-border px-4 py-3 text-muted-foreground">
                          {item.airlinePurpose}
                        </td>
                        <td className="border-b border-border px-4 py-3">
                          <div className="flex flex-wrap gap-1.5">
                            {item.primaryDatastores.map((store) => (
                              <Badge key={store} variant="outline">
                                {store}
                              </Badge>
                            ))}
                          </div>
                        </td>
                        <td className="border-b border-border px-4 py-3 text-xs text-muted-foreground">
                          {item.repoArtifacts.join(", ")}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
        </section>

        <section className="mb-8">
          <div className="mb-4 flex items-center gap-2">
            <BookOpenText className="h-4 w-4 text-primary" />
            <h2 className="font-display text-xl font-semibold">Source-by-Source Detail</h2>
          </div>
          <div className="grid gap-4 lg:grid-cols-2">
            {DATA_SHAPE_DETAILS.map((shape) => (
              <Card key={shape.id} className="border-border/80 bg-card/90">
                <CardHeader className="space-y-2">
                  <CardTitle className="text-lg">{shape.title}</CardTitle>
                  <CardDescription>{shape.format}</CardDescription>
                </CardHeader>
                <CardContent className="space-y-4 text-sm">
                  <div>
                    <p className="mb-2 text-xs font-semibold uppercase tracking-[0.1em] text-muted-foreground">
                      Key fields
                    </p>
                    <div className="flex flex-wrap gap-1.5">
                      {shape.keyFields.map((field) => (
                        <span
                          key={field}
                          className="rounded-md border border-border bg-surface-2 px-2 py-1 text-xs text-foreground"
                        >
                          {field}
                        </span>
                      ))}
                    </div>
                  </div>

                  <div>
                    <p className="mb-1 text-xs font-semibold uppercase tracking-[0.1em] text-muted-foreground">
                      Example
                    </p>
                    <p className="rounded-md border border-border bg-surface-1 px-3 py-2 font-mono text-xs text-muted-foreground">
                      {shape.exampleRecord}
                    </p>
                  </div>

                  <div>
                    <p className="mb-1 text-xs font-semibold uppercase tracking-[0.1em] text-muted-foreground">
                      Retrieval role
                    </p>
                    <ul className="list-disc space-y-1 pl-5 text-muted-foreground">
                      {shape.retrievalUse.map((item) => (
                        <li key={item}>{item}</li>
                      ))}
                    </ul>
                  </div>

                  <div>
                    <p className="mb-1 text-xs font-semibold uppercase tracking-[0.1em] text-muted-foreground">
                      Repo artifacts
                    </p>
                    <ul className="list-disc space-y-1 pl-5 text-muted-foreground">
                      {shape.repoArtifacts.map((artifact) => (
                        <li key={artifact}>
                          <code>{artifact}</code>
                        </li>
                      ))}
                    </ul>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        </section>

        <section className="mb-8 grid gap-4 xl:grid-cols-5">
          <Card className="xl:col-span-3">
            <CardHeader>
              <div className="mb-1 flex items-center gap-2">
                <Network className="h-4 w-4 text-primary" />
                <CardTitle className="text-xl">Context to Datastore Mapping</CardTitle>
              </div>
              <CardDescription>
                Retrieval planning selects the store and mode based on the type of question.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-3 text-sm">
              {CONTEXT_TO_DATASTORE_MAPPING.map((item, index) => (
                <div key={item.contextFamily}>
                  <div className="mb-1 flex flex-wrap items-center gap-2">
                    <span className="font-semibold text-foreground">{item.contextFamily}</span>
                    <Badge variant="outline">{item.datastore}</Badge>
                  </div>
                  <p className="text-muted-foreground">
                    <span className="font-medium text-foreground">Mode:</span> {item.retrievalMode}
                  </p>
                  <p className="text-muted-foreground">{item.rationale}</p>
                  {index < CONTEXT_TO_DATASTORE_MAPPING.length - 1 && (
                    <Separator className="mt-3" />
                  )}
                </div>
              ))}
            </CardContent>
          </Card>

          <Card className="xl:col-span-2">
            <CardHeader>
              <div className="mb-1 flex items-center gap-2">
                <Radar className="h-4 w-4 text-primary" />
                <CardTitle className="text-xl">Agentic Retrieval Flow</CardTitle>
              </div>
              <CardDescription>
                Deterministic retrieval first, reasoning second, confidence explicit.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <ol className="space-y-3 text-sm">
                {RETRIEVAL_FLOW_STEPS.map((step, index) => (
                  <li key={step.step} className="flex items-start gap-3">
                    <span className="inline-flex h-6 w-6 shrink-0 items-center justify-center rounded-full border border-primary/30 bg-primary/10 text-xs font-semibold text-primary">
                      {index + 1}
                    </span>
                    <div>
                      <p className="font-semibold text-foreground">{step.step}</p>
                      <p className="text-muted-foreground">{step.description}</p>
                    </div>
                  </li>
                ))}
              </ol>
            </CardContent>
          </Card>
        </section>

        <section className="mb-2">
          <Card>
            <CardHeader>
              <CardTitle className="text-xl">Glossary</CardTitle>
              <CardDescription>
                Shared acronyms used across retrieval plans and pilot-brief outputs.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
                {GLOSSARY_ITEMS.map((item) => (
                  <div
                    key={item.acronym}
                    className="rounded-lg border border-border bg-surface-1/40 p-3"
                  >
                    <div className="mb-1 flex items-center gap-2">
                      <Badge variant="outline">{item.acronym}</Badge>
                      <span className="text-sm font-semibold text-foreground">
                        {item.longForm}
                      </span>
                    </div>
                    <p className="text-sm text-muted-foreground">{item.whyItMatters}</p>
                  </div>
                ))}
              </div>
            </CardContent>
          </Card>
        </section>
      </div>
    </div>
  );
}
