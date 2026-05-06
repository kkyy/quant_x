import { useState, useRef, useEffect } from "react";
import { X } from "lucide-react";
import { clsx } from "clsx";

export interface MultiSelectOption {
  value: string;
  label: string;
}

interface MultiSelectProps {
  options: MultiSelectOption[];
  values: string[];
  onChange: (values: string[]) => void;
  placeholder?: string;
  className?: string;
}

export function MultiSelect({
  options,
  values,
  onChange,
  placeholder = "Select...",
  className,
}: MultiSelectProps) {
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  const toggle = (value: string) => {
    if (values.includes(value)) {
      onChange(values.filter((v) => v !== value));
    } else {
      onChange([...values, value]);
    }
  };

  return (
    <div ref={ref} className={clsx("relative", className)}>
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className={clsx(
          "w-full min-h-9 px-3 py-2 text-left",
          "bg-terminal-surface border border-terminal-border rounded-sm",
          "text-xs font-mono",
          "hover:border-terminal-text-dim focus:outline-none focus:border-terminal-green",
          "flex flex-wrap gap-1 items-center transition-colors"
        )}
      >
        {values.length === 0 ? (
          <span className="text-terminal-text-dim">{placeholder}</span>
        ) : (
          values.map((v) => {
            const opt = options.find((o) => o.value === v);
            return (
              <span
                key={v}
                className="inline-flex items-center gap-1 px-1.5 py-0.5 bg-terminal-green-glow border border-terminal-green/30 text-terminal-green text-[10px] rounded-sm"
              >
                {opt?.label ?? v}
                <button
                  type="button"
                  onClick={(e) => {
                    e.stopPropagation();
                    toggle(v);
                  }}
                  className="hover:text-white transition-colors"
                >
                  <X size={10} />
                </button>
              </span>
            );
          })
        )}
      </button>

      {open && (
        <div className="absolute z-50 w-full mt-1 bg-terminal-surface border border-terminal-border rounded-sm shadow-2xl max-h-60 overflow-y-auto">
          {options.map((opt) => {
            const selected = values.includes(opt.value);
            return (
              <button
                key={opt.value}
                type="button"
                onClick={() => toggle(opt.value)}
                className="w-full px-3 py-2 text-left text-xs font-mono hover:bg-terminal-raised flex items-center gap-2 text-terminal-text"
              >
                <span
                  className={clsx(
                    "w-3.5 h-3.5 border rounded-sm flex items-center justify-center flex-shrink-0",
                    selected
                      ? "bg-terminal-green border-terminal-green"
                      : "border-terminal-border bg-transparent"
                  )}
                >
                  {selected && (
                    <svg viewBox="0 0 12 12" fill="none" className="w-2.5 h-2.5 text-terminal-bg">
                      <path
                        d="M2 6l3 3 5-5"
                        stroke="currentColor"
                        strokeWidth="2"
                        strokeLinecap="round"
                        strokeLinejoin="round"
                      />
                    </svg>
                  )}
                </span>
                {opt.label}
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}
