import { PIPELINE_STEPS } from "@/data/data-sources-reference";
import { Card, CardContent } from "@/components/ui/card";
import { SectionHeading } from "./section-heading";
import { Workflow } from "lucide-react";

export function PipelineTable() {
  return (
    <section className="mb-10">
      <SectionHeading id="pipeline" icon={Workflow} title="Data Pipeline" />
      <p className="mb-4 text-sm text-muted-foreground">
        Numbered scripts in <code className="rounded bg-surface-2 px-1.5 py-0.5 font-mono text-xs text-primary">scripts/</code> form
        the ETL pipeline. Each script reads from upstream data sources or intermediate files and loads into one or more target stores.
      </p>
      <Card>
        <CardContent className="p-0">
          <div className="overflow-x-auto">
            <table className="w-full min-w-[600px] border-collapse text-sm">
              <thead>
                <tr className="bg-surface-2 text-left">
                  <th className="border-b border-border px-4 py-3 font-semibold">Script</th>
                  <th className="border-b border-border px-4 py-3 font-semibold">Description</th>
                  <th className="border-b border-border px-4 py-3 font-semibold">Target Store</th>
                </tr>
              </thead>
              <tbody>
                {PIPELINE_STEPS.map((step) => (
                  <tr key={step.scriptNum} className="align-top odd:bg-card even:bg-surface-1/30">
                    <td className="border-b border-border px-4 py-2.5 font-mono font-bold text-primary">
                      {step.scriptNum}
                    </td>
                    <td className="border-b border-border px-4 py-2.5 text-muted-foreground">
                      {step.description}
                    </td>
                    <td className="border-b border-border px-4 py-2.5 font-mono text-xs text-blue-400">
                      {step.targetStore}
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
