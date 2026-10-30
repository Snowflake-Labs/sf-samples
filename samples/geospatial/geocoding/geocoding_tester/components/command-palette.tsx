"use client"

// ⌘K / Ctrl+K command palette (the hint shows the right one per platform). Pages come from NAV_ITEMS in lib/constants.ts;
// add your own actions to the "Actions" group.

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { useQueryClient } from "@tanstack/react-query"
import { ArrowRight, Command as CommandIcon, Moon, RefreshCw, Sparkles, Sun } from "lucide-react"
import { NAV_ITEMS } from "@/lib/constants"
import { copyText } from "@/lib/clipboard"
import { useTheme } from "@/components/theme-provider"
import { Button } from "@/components/ui/button"
import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
} from "@/components/ui/command"

export function CommandPalette() {
  const [open, setOpen] = useState(false)
  const router = useRouter()
  const queryClient = useQueryClient()
  const { resolvedTheme, setTheme } = useTheme()
  const [shortcut, setShortcut] = useState("Ctrl K")

  useEffect(() => {
    if (/Mac|iPhone|iPad/.test(navigator.platform || navigator.userAgent)) setShortcut("⌘K")
  }, [])

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key.toLowerCase() === "k" && (e.metaKey || e.ctrlKey)) {
        e.preventDefault()
        setOpen((o) => !o)
      }
    }
    document.addEventListener("keydown", onKey)
    return () => document.removeEventListener("keydown", onKey)
  }, [])

  const run = (fn: () => void) => () => {
    setOpen(false)
    fn()
  }

  return (
    <>
      <Button
        variant="outline"
        onClick={() => setOpen(true)}
        className="h-9 gap-2 px-2.5 text-muted-foreground sm:w-56 sm:justify-start sm:px-3"
        aria-label="Open command palette"
      >
        <CommandIcon className="h-4 w-4" />
        <span className="hidden text-sm font-normal sm:inline">Commands</span>
        <kbd className="pointer-events-none ml-auto hidden rounded border bg-muted px-1.5 font-mono text-[10px] font-medium sm:inline">
          {shortcut}
        </kbd>
      </Button>
      <CommandDialog open={open} onOpenChange={setOpen}>
        <CommandInput placeholder="Type a command…" />
        <CommandList>
          <CommandEmpty>No results found.</CommandEmpty>
          {NAV_ITEMS.length > 0 && (
            <>
              <CommandGroup heading="Go to">
                {NAV_ITEMS.map((item) => (
                  <CommandItem key={item.href} onSelect={run(() => router.push(item.href))}>
                    <ArrowRight />
                    {item.label}
                  </CommandItem>
                ))}
              </CommandGroup>
              <CommandSeparator />
            </>
          )}
          <CommandGroup heading="Actions">
            <CommandItem onSelect={run(() => setTheme(resolvedTheme === "dark" ? "light" : "dark"))}>
              {resolvedTheme === "dark" ? <Sun /> : <Moon />}
              Switch to {resolvedTheme === "dark" ? "light" : "dark"} mode
            </CommandItem>
            <CommandItem onSelect={run(() => queryClient.invalidateQueries())}>
              <RefreshCw />
              Refresh data
            </CommandItem>
          </CommandGroup>
        </CommandList>
      </CommandDialog>
    </>
  )
}
