import { clsx } from "clsx";

type BadgeVariant = "neutral" | "info" | "success" | "error" | "warning";

interface BadgeProps {
  variant?: BadgeVariant;
  children: React.ReactNode;
  className?: string;
}

const variantClasses: Record<BadgeVariant, string> = {
  neutral: "bg-zinc-700 text-zinc-300",
  info: "bg-blue-600 text-white",
  success: "bg-emerald-600 text-white",
  error: "bg-red-600 text-white",
  warning: "bg-amber-600 text-white",
};

export function Badge({ variant = "neutral", children, className }: BadgeProps) {
  return (
    <span
      className={clsx(
        "inline-flex items-center px-2 py-0.5 text-xs font-medium rounded-full",
        variantClasses[variant],
        className
      )}
    >
      {children}
    </span>
  );
}
