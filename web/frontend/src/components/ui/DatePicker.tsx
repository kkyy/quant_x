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
        "w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg",
        "text-sm text-zinc-200",
        "focus:outline-none focus:ring-1 focus:ring-zinc-500 focus:border-zinc-500",
        "hover:border-zinc-600",
        "[color-scheme:dark]",
        className
      )}
    />
  );
}
