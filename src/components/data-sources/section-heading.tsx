import type { LucideIcon } from "lucide-react";

interface SectionHeadingProps {
  id: string;
  icon: LucideIcon;
  title: string;
}

export function SectionHeading({ id, icon: Icon, title }: SectionHeadingProps) {
  return (
    <div id={id} className="mb-4 flex scroll-mt-20 items-center gap-2">
      <Icon className="h-5 w-5 text-primary" />
      <h2 className="font-display text-xl font-semibold">{title}</h2>
    </div>
  );
}
