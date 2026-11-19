import { cn } from '@/lib/utils';
import type React from 'react';

/**
 * PageHeader — title block at the top of a page.
 *   <PageHeader title="Orders" description="Last 30 days" actions={<Button>Export</Button>} />
 */
export function PageHeader({
  title,
  description,
  eyebrow,
  actions,
  className,
}: {
  title: React.ReactNode;
  description?: React.ReactNode;
  eyebrow?: React.ReactNode;
  actions?: React.ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cn('flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between', className)}
    >
      <div className="space-y-1.5">
        {eyebrow && (
          <p className="eyebrow text-xs">{eyebrow}</p>
        )}
        <h1 className="text-4xl font-extrabold tracking-tight text-white sm:text-5xl">{title}</h1>
        {description && (
          <p className="max-w-2xl text-base leading-snug text-muted-foreground sm:text-lg">{description}</p>
        )}
      </div>
      {actions && <div className="flex shrink-0 items-center gap-2">{actions}</div>}
    </div>
  );
}
