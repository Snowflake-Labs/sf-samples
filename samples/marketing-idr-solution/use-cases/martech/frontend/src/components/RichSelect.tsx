import React, { useState, useEffect, useRef } from 'react';
import './RichSelect.css';

export interface RichSelectOption {
  value: string;
  label?: string;
  description?: string;
}

interface RichSelectProps {
  value: string;
  options: RichSelectOption[];
  onChange: (value: string) => void;
  icon?: React.ReactNode;
  placeholder?: string;
}

/**
 * A Stellar-style dropdown with optional descriptions per option.
 * Supports simple string arrays (no description) or full RichSelectOption objects.
 *
 * Usage:
 *   <RichSelect value={val} options={[{ value: 'a', label: 'A', description: '...' }]} onChange={setVal} />
 *   <RichSelect value={val} options={[{ value: 'a' }, { value: 'b' }]} onChange={setVal} />
 */
export default function RichSelect({ value, options, onChange, icon, placeholder }: RichSelectProps) {
  const [open, setOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [open]);

  const selected = options.find(o => o.value === value);
  const displayLabel = selected?.label || selected?.value || placeholder || '';

  return (
    <div className="rich-select" ref={containerRef}>
      <button
        type="button"
        className={`rich-select-trigger ${open ? 'rich-select-trigger--open' : ''}`}
        onClick={() => setOpen(!open)}
      >
        {icon && <span className="rich-select-icon">{icon}</span>}
        <span className="rich-select-value">{displayLabel}</span>
        <svg width="12" height="12" viewBox="0 0 12 12" className="rich-select-chevron">
          <path d="M3 4.5L6 7.5L9 4.5" stroke="currentColor" strokeWidth="1.5" fill="none" strokeLinecap="round" strokeLinejoin="round"/>
        </svg>
      </button>
      {open && (
        <div className="rich-select-menu">
          {options.map(opt => {
            const isActive = opt.value === value;
            return (
              <button
                key={opt.value}
                type="button"
                className={`rich-select-item ${isActive ? 'rich-select-item--active' : ''}`}
                onClick={() => { onChange(opt.value); setOpen(false); }}
              >
                <div className="rich-select-item-row">
                  <span className="rich-select-item-label">{opt.label || opt.value}</span>
                  {isActive && (
                    <svg width="16" height="16" viewBox="0 0 16 16" className="rich-select-check">
                      <path d="M3.5 8.5L6.5 11.5L12.5 4.5" stroke="currentColor" strokeWidth="2" fill="none" strokeLinecap="round" strokeLinejoin="round"/>
                    </svg>
                  )}
                </div>
                {opt.description && (
                  <span className="rich-select-item-desc">{opt.description}</span>
                )}
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}
