import type { ReactNode } from 'react';
import { Info } from 'lucide-react';

export function DashStatCard({
  label,
  value,
  desc,
  info,
  className,
}: {
  label: string;
  value: ReactNode;
  desc: ReactNode;
  info?: { id: string; ariaLabel: string; content: ReactNode; tooltipClassName?: string };
  className?: string;
}) {
  return (
    <div className={`dash-stat-card-uniform${className ? ` ${className}` : ''}`}>
      <div className="dash-stat-card-uniform-head">
        <span className="dash-stat-card-uniform-label">{label}</span>
        {info && (
          <div className="panel-hint-wrap">
            <button
              type="button"
              className="panel-hint-btn"
              aria-label={info.ariaLabel}
              aria-describedby={info.id}
            >
              <Info size={15} strokeWidth={2} aria-hidden />
            </button>
            <div
              id={info.id}
              className={`panel-hint-tooltip panel-hint-tooltip--below${info.tooltipClassName ? ` ${info.tooltipClassName}` : ''}`}
              role="tooltip"
            >
              {info.content}
            </div>
          </div>
        )}
      </div>
      <div className="dash-stat-card-uniform-value">{value}</div>
      <p className="dash-stat-card-uniform-desc">{desc}</p>
    </div>
  );
}
