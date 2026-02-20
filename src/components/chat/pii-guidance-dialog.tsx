"use client";

import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { HelpCircle, ShieldCheck, ShieldX, Info } from "lucide-react";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";

const BLOCKED_EXAMPLES = [
  { pattern: "123-45-6789", description: "Social Security Number" },
  { pattern: "4111-1111-1111-1111", description: "Credit Card Number" },
  { pattern: "5500 0000 0000 0004", description: "Credit Card (Mastercard)" },
  { pattern: "GB82 WEST 1234 5698 7654 32", description: "IBAN" },
  { pattern: "DEUTDEFF", description: "SWIFT/BIC Code" },
  { pattern: "john.doe@gmail.com", description: "Email Address" },
  { pattern: "+1-555-123-4567", description: "Phone Number" },
  { pattern: "123 Main St, Boston, MA 02101", description: "Physical Address" },
  { pattern: "021000021", description: "ABA Routing Number" },
];

const ALLOWED_EXAMPLES = [
  { pattern: "I have $100M to invest", description: "Dollar amounts (not PII)" },
  { pattern: "My salary is $500,000", description: "Income statements" },
  { pattern: "I'm worth $50 million", description: "Wealth statements" },
  { pattern: "High risk appetite", description: "Investment preferences" },
  { pattern: "Which funds hold NVIDIA?", description: "Fund queries" },
  { pattern: "Compare bond fund duration", description: "Analysis requests" },
  { pattern: "What does IMF say about rates?", description: "Market questions" },
];

export function PiiGuidanceDialog() {
  return (
    <Dialog>
      <DialogTrigger asChild>
        <Button
          variant="ghost"
          size="icon"
          className="h-6 w-6 rounded-full text-muted-foreground hover:text-foreground hover:bg-surface-3"
          aria-label="PII filter information"
        >
          <HelpCircle className="h-3.5 w-3.5" />
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-[500px]">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <ShieldCheck className="h-5 w-5 text-emerald-500" />
            PII Protection Guide
          </DialogTitle>
          <DialogDescription>
            Your messages are scanned for sensitive personal information before being processed.
          </DialogDescription>
        </DialogHeader>

        <ScrollArea className="max-h-[400px] pr-4">
          <div className="space-y-6">
            {/* Blocked Section */}
            <div>
              <div className="flex items-center gap-2 mb-3">
                <ShieldX className="h-4 w-4 text-red-500" />
                <h3 className="font-semibold text-sm text-red-600 dark:text-red-400">
                  Blocked (Will Not Send)
                </h3>
              </div>
              <div className="space-y-2">
                {BLOCKED_EXAMPLES.map((example, idx) => (
                  <div
                    key={idx}
                    className="flex items-start gap-3 p-2 rounded-lg bg-red-500/5 border border-red-500/20"
                  >
                    <code className="text-xs bg-red-500/10 px-2 py-1 rounded font-mono text-red-600 dark:text-red-400 shrink-0">
                      {example.pattern.length > 20
                        ? example.pattern.substring(0, 20) + "..."
                        : example.pattern}
                    </code>
                    <span className="text-xs text-muted-foreground">
                      {example.description}
                    </span>
                  </div>
                ))}
              </div>
            </div>

            {/* Allowed Section */}
            <div>
              <div className="flex items-center gap-2 mb-3">
                <ShieldCheck className="h-4 w-4 text-emerald-500" />
                <h3 className="font-semibold text-sm text-emerald-600 dark:text-emerald-400">
                  Allowed (Will Send)
                </h3>
              </div>
              <div className="space-y-2">
                {ALLOWED_EXAMPLES.map((example, idx) => (
                  <div
                    key={idx}
                    className="flex items-start gap-3 p-2 rounded-lg bg-emerald-500/5 border border-emerald-500/20"
                  >
                    <code className="text-xs bg-emerald-500/10 px-2 py-1 rounded font-mono text-emerald-600 dark:text-emerald-400 shrink-0">
                      {example.pattern.length > 25
                        ? example.pattern.substring(0, 25) + "..."
                        : example.pattern}
                    </code>
                    <span className="text-xs text-muted-foreground">
                      {example.description}
                    </span>
                  </div>
                ))}
              </div>
            </div>

            {/* Info Note */}
            <div className="flex gap-2 p-3 rounded-lg bg-blue-500/5 border border-blue-500/20">
              <Info className="h-4 w-4 text-blue-500 shrink-0 mt-0.5" />
              <p className="text-xs text-muted-foreground">
                <strong className="text-foreground">Note:</strong> The filter detects specific patterns
                (account numbers, SSNs, etc.), not semantic meaning. General financial discussions
                like investment amounts or salary ranges are permitted.
              </p>
            </div>
          </div>
        </ScrollArea>
      </DialogContent>
    </Dialog>
  );
}
