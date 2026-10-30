import Image from "next/image"
import Link from "next/link"
import { APP_TITLE, LOGO_SRC } from "@/lib/constants"
import { ThemeToggle } from "@/components/theme-toggle"
import { CommandPalette } from "@/components/command-palette"

/**
 * AppHeader — top navigation bar.
 * To customize: edit APP_TITLE, LOGO_SRC, and NAV_ITEMS in lib/constants.ts.
 */
export function AppHeader() {
  return (
    <header className="sticky top-0 z-40 w-full border-b border-border/70 bg-background/75 text-foreground backdrop-blur-lg supports-[backdrop-filter]:bg-background/60">
      <div className="mx-auto flex h-14 w-full max-w-7xl items-center gap-3 px-4 sm:px-6 lg:px-8">
        <Link href="/" className="flex min-w-0 items-center gap-2.5">
          {LOGO_SRC && (
            <Image
              src={LOGO_SRC}
              alt={`${APP_TITLE} logo`}
              width={28}
              height={28}
              className="shrink-0"
            />
          )}
          <span className="truncate text-sm font-semibold tracking-tight">{APP_TITLE}</span>
        </Link>
        <div className="ml-auto flex shrink-0 items-center gap-1.5">
          <CommandPalette />
          <ThemeToggle />
        </div>
      </div>
    </header>
  )
}
