"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { useTheme } from "next-themes";
import { PlaneTakeoff, Moon, Sun, Database } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { getDataAsOfTimestamp } from "@/data/seed";

export function Header() {
  const { theme, setTheme } = useTheme();
  const [mounted, setMounted] = useState(false);
  const [timestamp, setTimestamp] = useState<string>("Loading...");

  useEffect(() => {
    setMounted(true);
    setTimestamp(getDataAsOfTimestamp());
  }, []);

  return (
    <header className="border-b border-primary/20 bg-gradient-to-r from-primary to-primary/90 text-primary-foreground">
      <div className="flex h-14 items-center justify-between px-4 md:px-6">
        <div className="flex items-center gap-3">
          <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-white/15 ring-1 ring-white/25">
            <PlaneTakeoff className="h-4 w-4" />
          </div>
          <p className="font-display text-sm font-semibold tracking-wide">
            AeroLynx Pilot Brief Bot
          </p>
        </div>

        <div className="flex items-center gap-1">
          <span className="hidden text-[11px] text-primary-foreground/75 md:inline">
            Data as-of {timestamp}
          </span>
          <Button
            asChild
            variant="ghost"
            size="sm"
            className="hidden text-primary-foreground hover:bg-white/15 hover:text-white md:inline-flex"
          >
            <Link href="/data-sources">
              <Database className="h-4 w-4" />
              Data Sources
            </Link>
          </Button>

          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  asChild
                  variant="ghost"
                  size="icon-sm"
                  className="text-primary-foreground hover:bg-white/15 hover:text-white md:hidden"
                  aria-label="Open data sources"
                >
                  <Link href="/data-sources">
                    <Database className="h-4 w-4" />
                  </Link>
                </Button>
              </TooltipTrigger>
              <TooltipContent>
                <p>Open data sources</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>

          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  variant="ghost"
                  size="icon-sm"
                  className="text-primary-foreground hover:bg-white/15 hover:text-white"
                  onClick={() => setTheme(theme === "dark" ? "light" : "dark")}
                  aria-label="Toggle theme"
                >
                  <Sun className="h-4 w-4 rotate-0 scale-100 transition-transform dark:-rotate-90 dark:scale-0" />
                  <Moon className="absolute h-4 w-4 rotate-90 scale-0 transition-transform dark:rotate-0 dark:scale-100" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>
                <p>{mounted ? (theme === "dark" ? "Switch to light" : "Switch to dark") : "Toggle theme"}</p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
      </div>

    </header>
  );
}
