import { clsx } from "clsx";

interface DatePickerProps {
  value: string;
  onChange: (value: string) => void;
  className?: string;
}

export function DatePicker({ value, onChange, className }: DatePickerProps) {
  return (
    <input
      type="date"
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className={clsx(
        "w-full px-3 py-2 bg-terminal-surface border border-terminal-border rounded-sm",
        "text-xs font-mono text-terminal-text",
        "focus:outline-none focus:border-terminal-green hover:border-terminal-text-dim transition-colors",
        className
      )}
    />
  );
}
