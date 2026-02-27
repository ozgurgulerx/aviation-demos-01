import { RETRIEVAL_MODE_COMPARISON } from "@/data/data-sources-reference";
import { Card, CardContent } from "@/components/ui/card";
import { SectionHeading } from "./section-heading";
import { ArrowLeftRight } from "lucide-react";

export function RetrievalComparison() {
  return (
    <section className="mb-10">
      <SectionHeading id="modes" icon={ArrowLeftRight} title="Retrieval Modes Comparison" />
      <p className="mb-4 text-sm text-muted-foreground">
        Two retrieval modes are available in the UI toggle. Code-RAG is the full-featured
        agentic pipeline; Foundry IQ uses the Fabric Data Agent for managed retrieval.
      </p>
      <Card>
        <CardContent className="p-0">
          <div className="overflow-x-auto">
            <table className="w-full min-w-[700px] border-collapse text-sm">
              <thead>
                <tr className="bg-surface-2 text-left">
                  <th className="border-b border-border px-4 py-3 font-semibold" style={{ width: "25%" }}>
                    Dimension
                  </th>
                  <th className="border-b border-border px-4 py-3 font-semibold" style={{ width: "37.5%" }}>
                    Code-RAG (Default)
                  </th>
                  <th className="border-b border-border px-4 py-3 font-semibold" style={{ width: "37.5%" }}>
                    Foundry IQ (Preview)
                  </th>
                </tr>
              </thead>
              <tbody>
                {RETRIEVAL_MODE_COMPARISON.map((row) => (
                  <tr key={row.dimension} className="align-top odd:bg-card even:bg-surface-1/30">
                    <td className="border-b border-border px-4 py-2.5 font-semibold text-foreground">
                      {row.dimension}
                    </td>
                    <td className="border-b border-border px-4 py-2.5 text-muted-foreground">
                      {row.codeRag}
                    </td>
                    <td className="border-b border-border px-4 py-2.5 text-muted-foreground">
                      {row.foundryIq}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </section>
  );
}
