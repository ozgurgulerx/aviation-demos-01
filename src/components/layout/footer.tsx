import { Info } from "lucide-react";

export function Footer() {
  return (
    <footer className="border-t border-border bg-card/90 px-4 py-2">
      <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
        <Info className="h-3.5 w-3.5 shrink-0" />
        <span>
          Pilot brief guidance only. Dispatch authority remains with operational control teams.
        </span>
      </div>
    </footer>
  );
}
