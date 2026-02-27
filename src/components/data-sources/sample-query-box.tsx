import type { SampleQuery } from "@/data/data-sources-reference";

interface SampleQueryBoxProps {
  query: SampleQuery;
  accentColor: string;
}

export function SampleQueryBox({ query, accentColor }: SampleQueryBoxProps) {
  return (
    <div
      className="flex items-start justify-between gap-3 rounded-md border border-border bg-background/60 px-3 py-2"
      style={{ borderLeftWidth: 3, borderLeftColor: accentColor }}
    >
      <span className="text-sm text-muted-foreground">&ldquo;{query.query}&rdquo;</span>
      <span className="shrink-0 rounded bg-surface-2 px-2 py-0.5 font-mono text-[0.7rem] font-semibold text-foreground">
        {query.route}
      </span>
    </div>
  );
}
