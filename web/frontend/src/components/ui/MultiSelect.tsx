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
        className="w-full min-h-10 px-3 py-2 text-left bg-zinc-800 border border-zinc-700 rounded-lg text-sm text-zinc-300 hover:border-zinc-600 focus:outline-none focus:ring-1 focus:ring-zinc-500 flex flex-wrap gap-1 items-center"
      >
        {values.length === 0 ? (
          <span className="text-zinc-500">{placeholder}</span>
        ) : (
          values.map((v) => {
            const opt = options.find((o) => o.value === v);
            return (
              <span
                key={v}
                className="inline-flex items-center gap-1 px-2 py-0.5 bg-blue-600 text-white text-xs rounded-full"
              >
                {opt?.label ?? v}
                <button
                  type="button"
                  onClick={(e) => {
                    e.stopPropagation();
                    toggle(v);
                  }}
                  className="hover:text-blue-200"
                >
                  <X size={10} />
                </button>
              </span>
            );
          })
        )}
      </button>

      {open && (
        <div className="absolute z-50 w-full mt-1 bg-zinc-800 border border-zinc-700 rounded-lg shadow-xl max-h-60 overflow-y-auto">
          {options.map((opt) => {
            const selected = values.includes(opt.value);
            return (
              <button
                key={opt.value}
                type="button"
                onClick={() => toggle(opt.value)}
                className="w-full px-3 py-2 text-left text-sm hover:bg-zinc-700 flex items-center gap-2 text-zinc-300"
              >
                <span
                  className={clsx(
                    "w-4 h-4 border rounded flex items-center justify-center flex-shrink-0",
                    selected
                      ? "bg-blue-600 border-blue-600"
                      : "border-zinc-600 bg-transparent"
                  )}
                >
                  {selected && (
                    <svg
                      viewBox="0 0 12 12"
                      fill="none"
                      className="w-3 h-3 text-white"
                    >
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
