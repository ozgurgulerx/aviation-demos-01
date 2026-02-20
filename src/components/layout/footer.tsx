import { AlertTriangle, CircleHelp } from "lucide-react";

export function Footer() {
  return (
    <footer className="border-t border-border bg-card/90 px-4 py-2.5">
      <div className="flex flex-col items-start justify-between gap-2 text-xs text-muted-foreground sm:flex-row sm:items-center">
        <div className="flex items-center gap-1.5">
          <AlertTriangle className="h-3.5 w-3.5" />
          <span>
            Pilot brief guidance only. Dispatch authority remains with operational control teams.
          </span>
        </div>
        <div className="flex items-center gap-1.5">
          <CircleHelp className="h-3.5 w-3.5" />
          <span>
            PAIR/HAX aligned: transparency, confidence signaling, and source traceability by default.
          </span>
        </div>
      </div>
    </footer>
  );
}
