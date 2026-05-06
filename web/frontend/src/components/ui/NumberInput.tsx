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
        "w-full px-3 py-2 bg-zinc-800 border border-zinc-700 rounded-lg",
        "text-sm text-zinc-200 placeholder-zinc-500",
        "focus:outline-none focus:ring-1 focus:ring-zinc-500 focus:border-zinc-500",
        "hover:border-zinc-600",
        "[appearance:textfield] [&::-webkit-outer-spin-button]:appearance-none [&::-webkit-inner-spin-button]:appearance-none",
        className
      )}
    />
  );
}
