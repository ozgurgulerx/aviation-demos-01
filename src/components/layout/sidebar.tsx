"use client";

import Link from "next/link";
import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  MessageSquarePlus,
  Search,
  ChevronLeft,
  ChevronRight,
  MessageCircle,
  Star,
  Building2,
  Users,
  Bookmark,
  Radar,
  Database,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import {
  SAMPLE_CONVERSATIONS,
  SAMPLE_WATCHLIST,
  QUERY_CATEGORIES,
  type QueryCategory,
} from "@/data/seed";
import type { Conversation, WatchlistItem } from "@/types";

interface SidebarProps {
  isCollapsed: boolean;
  onToggle: () => void;
  onSelectConversation: (id: string) => void;
  activeConversationId?: string;
  onNewChat: () => void;
  onRunPreset?: (prompt: string) => void;
}

export function Sidebar({
  isCollapsed,
  onToggle,
  onSelectConversation,
  activeConversationId,
  onNewChat,
  onRunPreset,
}: SidebarProps) {
  const [searchQuery, setSearchQuery] = useState("");

  const filteredConversations = SAMPLE_CONVERSATIONS.filter((conversation) =>
    conversation.title.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const savedConversations = filteredConversations.filter((conversation) => conversation.isSaved);
  const recentConversations = filteredConversations.filter((conversation) => !conversation.isSaved);

  return (
    <motion.aside
      initial={false}
      animate={{ width: isCollapsed ? 64 : 312 }}
      transition={{ duration: 0.2, ease: "easeInOut" }}
      className="relative flex h-full flex-col border-r border-border bg-surface-1/80"
    >
      <Button
        variant="ghost"
        size="icon-sm"
        onClick={onToggle}
        className="absolute -right-3 top-16 z-10 h-6 w-6 rounded-full border border-border bg-background shadow-subtle"
        aria-label={isCollapsed ? "Expand sidebar" : "Collapse sidebar"}
      >
        {isCollapsed ? <ChevronRight className="h-3.5 w-3.5" /> : <ChevronLeft className="h-3.5 w-3.5" />}
      </Button>

      <div className="p-3">
        <Button
          onClick={onNewChat}
          variant="gold"
          className={cn("w-full justify-start gap-2", isCollapsed && "justify-center px-0")}
        >
          <MessageSquarePlus className="h-4 w-4" />
          {!isCollapsed && <span>New Brief</span>}
        </Button>
      </div>

      <AnimatePresence>
        {!isCollapsed && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="space-y-3 px-3 pb-3"
          >
            <div className="relative">
              <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
              <Input
                placeholder="Search brief sessions"
                value={searchQuery}
                onChange={(event) => setSearchQuery(event.target.value)}
                className="h-9 bg-card pl-9 text-sm"
              />
            </div>

            <div className="rounded-xl border border-primary/20 bg-primary/5 p-3 text-xs">
              <div className="mb-1.5 flex items-center justify-between gap-2">
                <span className="font-semibold text-primary">Mission Status</span>
                <Badge variant="success" className="text-[10px]">
                  Live
                </Badge>
              </div>
              <p className="text-muted-foreground">
                Intent mapping and evidence checks are active for this flight-brief demo.
              </p>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      <ScrollArea className="flex-1">
        <div className="space-y-4 p-3 pt-0">
          {!isCollapsed && savedConversations.length > 0 && (
            <Section title="Pinned Briefs" icon={<Bookmark className="h-3.5 w-3.5" />}>
              {savedConversations.map((conversation) => (
                <ConversationItem
                  key={conversation.id}
                  conversation={conversation}
                  isActive={conversation.id === activeConversationId}
                  onClick={() => onSelectConversation(conversation.id)}
                  isCollapsed={isCollapsed}
                />
              ))}
            </Section>
          )}

          {!isCollapsed && recentConversations.length > 0 && (
            <Section title="Recent Runs" icon={<MessageCircle className="h-3.5 w-3.5" />}>
              {recentConversations.map((conversation) => (
                <ConversationItem
                  key={conversation.id}
                  conversation={conversation}
                  isActive={conversation.id === activeConversationId}
                  onClick={() => onSelectConversation(conversation.id)}
                  isCollapsed={isCollapsed}
                />
              ))}
            </Section>
          )}

          {isCollapsed && (
            <div className="flex flex-col items-center gap-2 pt-2">
              {SAMPLE_CONVERSATIONS.slice(0, 4).map((conversation) => (
                <Button
                  key={conversation.id}
                  variant={conversation.id === activeConversationId ? "secondary" : "ghost"}
                  size="icon-sm"
                  onClick={() => onSelectConversation(conversation.id)}
                  className="h-8 w-8"
                >
                  {conversation.isSaved ? <Star className="h-4 w-4" /> : <MessageCircle className="h-4 w-4" />}
                </Button>
              ))}
              <Button asChild variant="ghost" size="icon-sm" className="h-8 w-8">
                <Link href="/data-sources" aria-label="Open data sources">
                  <Database className="h-4 w-4" />
                </Link>
              </Button>
            </div>
          )}

          <Separator className="my-1" />

          {!isCollapsed && (
            <Section title="Scenario Presets" icon={<Radar className="h-3.5 w-3.5" />}>
              {QUERY_CATEGORIES.map((category) => (
                <PresetItem key={category.id} category={category} onRunPreset={onRunPreset} />
              ))}
            </Section>
          )}

          {!isCollapsed && (
            <>
              <Separator className="my-1" />
              <Section title="Reference" icon={<Database className="h-3.5 w-3.5" />}>
                <Link
                  href="/data-sources"
                  className="flex w-full items-center justify-between rounded-lg border border-transparent px-2.5 py-2 text-left text-sm text-muted-foreground transition-colors hover:border-border hover:bg-surface-2 hover:text-foreground"
                >
                  <span className="font-medium text-foreground">Data Sources</span>
                  <span className="text-xs text-muted-foreground">Detail</span>
                </Link>
              </Section>

              <Separator className="my-1" />
              <Section title="Watchlist" icon={<Star className="h-3.5 w-3.5 text-gold" />}>
                {SAMPLE_WATCHLIST.map((item) => (
                  <WatchlistItemComponent key={item.id} item={item} />
                ))}
              </Section>
            </>
          )}
        </div>
      </ScrollArea>
    </motion.aside>
  );
}

function Section({
  title,
  icon,
  children,
}: {
  title: string;
  icon: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-1.5">
      <div className="flex items-center gap-1.5 px-2 text-[11px] font-semibold uppercase tracking-[0.12em] text-muted-foreground">
        {icon}
        <span>{title}</span>
      </div>
      {children}
    </div>
  );
}

function ConversationItem({
  conversation,
  isActive,
  onClick,
  isCollapsed,
}: {
  conversation: Conversation;
  isActive: boolean;
  onClick: () => void;
  isCollapsed: boolean;
}) {
  if (isCollapsed) return null;

  return (
    <button
      onClick={onClick}
      className={cn(
        "w-full rounded-lg border px-2.5 py-2 text-left text-sm transition-colors",
        isActive
          ? "border-primary/35 bg-primary/10 text-foreground"
          : "border-transparent text-muted-foreground hover:border-border hover:bg-surface-2 hover:text-foreground"
      )}
    >
      <div className="flex items-center gap-2">
        {conversation.isSaved && <Star className="h-3 w-3 shrink-0 text-gold" />}
        <span className="truncate">{conversation.title}</span>
      </div>
    </button>
  );
}

function PresetItem({
  category,
  onRunPreset,
}: {
  category: QueryCategory;
  onRunPreset?: (prompt: string) => void;
}) {
  const preset = category.examples[0];
  return (
    <button
      onClick={() => preset && onRunPreset?.(preset)}
      className="w-full rounded-lg border border-transparent px-2.5 py-2 text-left text-sm text-muted-foreground transition-colors hover:border-border hover:bg-surface-2 hover:text-foreground"
    >
      <p className="font-medium text-foreground">{category.title}</p>
      <p className="line-clamp-2 text-xs">{preset}</p>
    </button>
  );
}

function WatchlistItemComponent({ item }: { item: WatchlistItem }) {
  return (
    <button className="w-full rounded-lg border border-transparent px-2.5 py-2 text-left text-sm text-muted-foreground transition-colors hover:border-border hover:bg-surface-2 hover:text-foreground">
      <div className="flex items-center gap-2">
        {item.type === "company" ? (
          <Building2 className="h-3.5 w-3.5 shrink-0" />
        ) : (
          <Users className="h-3.5 w-3.5 shrink-0" />
        )}
        <span className="truncate">{item.name}</span>
      </div>
    </button>
  );
}
