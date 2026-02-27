import { SUMMARY_STATS } from "@/data/data-sources-reference";

export function HeroStats() {
  return (
    <div className="flex flex-wrap justify-center gap-6 sm:gap-10">
      {SUMMARY_STATS.map((stat) => (
        <div key={stat.label} className="text-center">
          <span className="block font-display text-3xl font-bold text-primary">
            {stat.value}
          </span>
          <span className="text-xs uppercase tracking-wider text-muted-foreground">
            {stat.label}
          </span>
        </div>
      ))}
    </div>
  );
}
