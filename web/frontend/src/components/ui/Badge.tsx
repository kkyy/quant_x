import { clsx } from "clsx";

type BadgeVariant = "neutral" | "info" | "success" | "error" | "warning";

interface BadgeProps {
  variant?: BadgeVariant;
  children: React.ReactNode;
  className?: string;
}

const variantClasses: Record<BadgeVariant, string> = {
  neutral: "border-terminal-border text-terminal-text-dim",
  info: "border-terminal-cyan text-terminal-cyan",
  success: "border-terminal-green text-terminal-green",
  error: "border-terminal-red text-terminal-red",
  warning: "border-terminal-amber text-terminal-amber",
};

const variantBg: Record<BadgeVariant, string> = {
  neutral: "bg-terminal-raised",
  info: "bg-terminal-cyan-glow",
  success: "bg-terminal-green-glow",
  error: "bg-terminal-red-glow",
  warning: "bg-terminal-amber-glow",
};

export function Badge({ variant = "neutral", children, className }: BadgeProps) {
  return (
    <span
      className={clsx(
        "inline-flex items-center px-2 py-0.5 text-[10px] font-mono font-medium uppercase tracking-wider border rounded-sm",
        variantClasses[variant],
        variantBg[variant],
        className
      )}
    >
      {children}
    </span>
  );
}
