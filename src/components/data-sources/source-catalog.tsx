import { DATA_SOURCES } from "@/data/data-sources-reference";
import { SectionHeading } from "./section-heading";
import { SourceCard } from "./source-card";
import { Database } from "lucide-react";

export function SourceCatalog() {
  return (
    <section className="mb-10">
      <SectionHeading id="sources" icon={Database} title="Source Catalog" />
      <div className="space-y-6">
        {DATA_SOURCES.map((source) => (
          <SourceCard key={source.id} source={source} />
        ))}
      </div>
    </section>
  );
}
