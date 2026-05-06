import { useState, useEffect, useRef } from 'react';
import { ChevronDown } from 'lucide-react';
import { clsx } from 'clsx';

export interface SelectOption {
  value: string;
  label: string;
}

interface SelectProps {
  options: SelectOption[];
  value: string;
  onChange: (value: string) => void;
  placeholder?: string;
  searchable?: boolean;
  className?: string;
}

export function Select({
  options,
  value,
  onChange,
  placeholder = 'Select...',
  searchable = false,
  className = '',
}: SelectProps) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState('');
  const containerRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  const selected = options.find((o) => o.value === value);

  const filtered = searchable
    ? options.filter((o) => o.label.toLowerCase().includes(search.toLowerCase()))
    : options;

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false);
        setSearch('');
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  useEffect(() => {
    if (open && searchable && inputRef.current) {
      inputRef.current.focus();
    }
  }, [open, searchable]);

  const handleSelect = (val: string) => {
    onChange(val);
    setOpen(false);
    setSearch('');
  };

  return (
    <div ref={containerRef} className={clsx("relative", className)}>
      <button
        type="button"
        onClick={() => setOpen((v) => { if (!v) setSearch(''); return !v; })}
        className={clsx(
          "w-full flex items-center justify-between gap-2 px-3 py-2",
          "bg-terminal-surface border border-terminal-border rounded-sm",
          "text-xs font-mono",
          selected ? "text-terminal-text" : "text-terminal-text-dim",
          "hover:border-terminal-text-dim transition-colors focus-ring"
        )}
      >
        <span>{selected ? selected.label : placeholder}</span>
        <ChevronDown
          className={clsx(
            "w-3.5 h-3.5 text-terminal-text-dim shrink-0 transition-transform",
            open && "rotate-180"
          )}
        />
      </button>

      {open && (
        <div className="absolute z-50 mt-1 w-full bg-terminal-surface border border-terminal-border rounded-sm shadow-2xl overflow-hidden">
          {searchable && (
            <div className="p-2 border-b border-terminal-border-dim">
              <input
                ref={inputRef}
                type="text"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search..."
                className="w-full px-2 py-1.5 bg-terminal-bg border border-terminal-border rounded-sm text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim focus:outline-none focus:border-terminal-green"
              />
            </div>
          )}
          <ul className="max-h-60 overflow-y-auto py-1">
            {filtered.length === 0 ? (
              <li className="px-3 py-2 text-xs font-mono text-terminal-text-dim">NO OPTIONS</li>
            ) : (
              filtered.map((opt) => (
                <li key={opt.value}>
                  <button
                    type="button"
                    onClick={() => handleSelect(opt.value)}
                    className={clsx(
                      "w-full text-left px-3 py-2 text-xs font-mono transition-colors",
                      opt.value === value
                        ? "bg-terminal-green-glow text-terminal-green"
                        : "text-terminal-text hover:bg-terminal-raised"
                    )}
                  >
                    {opt.label}
                  </button>
                </li>
              ))
            )}
          </ul>
        </div>
      )}
    </div>
  );
}
