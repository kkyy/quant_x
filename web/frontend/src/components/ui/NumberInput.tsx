import { clsx } from "clsx";

interface NumberInputProps {
  value: number | undefined;
  onChange: (value: number | undefined) => void;
  step?: number;
  min?: number;
  max?: number;
  placeholder?: string;
  className?: string;
}

export function NumberInput({
  value,
  onChange,
  step = 1,
  min,
  max,
  placeholder,
  className,
}: NumberInputProps) {
  return (
    <input
      type="number"
      value={value ?? ""}
      step={step}
      min={min}
      max={max}
      placeholder={placeholder}
      onChange={(e) => {
        const raw = e.target.value;
        if (raw === "") {
          onChange(undefined);
          return;
        }
        const parsed = Number(raw);
        if (!isNaN(parsed)) {
          onChange(parsed);
        }
      }}
      className={clsx(
        "w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm",
        "text-xs font-mono text-terminal-text placeholder:text-terminal-text-dim",
        "focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors",
        className
      )}
    />
  );
}
