import { SOURCE_RELATIONSHIPS } from "@/data/data-sources-reference";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { SectionHeading } from "./section-heading";
import { GitBranch } from "lucide-react";

export function SourceRelationships() {
  return (
    <section className="mb-10">
      <SectionHeading id="relationships" icon={GitBranch} title="Source Relationships" />
      <p className="mb-4 text-sm text-muted-foreground">
        Cross-references and shared keys that connect data across sources. The GRAPH source
        materializes most of these relationships as traversable edges.
      </p>
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-base">Cross-Source Links</CardTitle>
        </CardHeader>
        <CardContent className="p-0">
          <div className="divide-y divide-border">
            {SOURCE_RELATIONSHIPS.map((rel, i) => (
              <div key={i} className="flex flex-wrap items-center gap-2 px-4 py-2.5 text-sm">
                <span className={`font-mono font-semibold ${rel.srcColor}`}>{rel.srcId}</span>
                <span className="font-mono text-xs text-primary">&larr; {rel.sharedKey} &rarr;</span>
                <span className={`font-mono font-semibold ${rel.dstColor}`}>{rel.dstId}</span>
                <span className="ml-auto text-xs text-muted-foreground">{rel.description}</span>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </section>
  );
}
