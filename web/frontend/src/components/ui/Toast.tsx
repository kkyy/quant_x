import { useState, useCallback, useEffect } from "react";
import { X, CheckCircle, XCircle, Info } from "lucide-react";
import { clsx } from "clsx";
import { motion, AnimatePresence } from "framer-motion";

type ToastVariant = "success" | "error" | "info";

interface ToastItem {
  id: string;
  variant: ToastVariant;
  message: string;
  createdAt: number;
}

const MAX_TOASTS = 5;
const DISMISS_MS: Record<ToastVariant, number> = {
  success: 3000,
  error: 6000,
  info: 4000,
};

let addToastFn: ((variant: ToastVariant, message: string) => void) | null = null;

export function toast(variant: ToastVariant, message: string) {
  addToastFn?.(variant, message);
}

export function ToastContainer() {
  const [toasts, setToasts] = useState<ToastItem[]>([]);

  const addToast = useCallback((variant: ToastVariant, message: string) => {
    const id = Math.random().toString(36).slice(2);
    setToasts((prev) => {
      const next = [...prev, { id, variant, message, createdAt: Date.now() }];
      return next.slice(-MAX_TOASTS);
    });
  }, []);

  useEffect(() => {
    addToastFn = addToast;
    return () => {
      addToastFn = null;
    };
  }, [addToast]);

  const remove = useCallback((id: string) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  }, []);

  return (
    <div className="fixed top-4 right-4 z-[100] flex flex-col gap-2 pointer-events-none">
      <AnimatePresence>
        {toasts.map((t) => (
          <ToastItem key={t.id} toast={t} onDismiss={remove} />
        ))}
      </AnimatePresence>
    </div>
  );
}

function ToastItem({
  toast: t,
  onDismiss,
}: {
  toast: ToastItem;
  onDismiss: (id: string) => void;
}) {
  useEffect(() => {
    const timer = setTimeout(() => onDismiss(t.id), DISMISS_MS[t.variant]);
    return () => clearTimeout(timer);
  }, [t.id, t.variant, onDismiss]);

  const accentMap: Record<ToastVariant, string> = {
    success: "border-l-terminal-green",
    error: "border-l-terminal-red",
    info: "border-l-terminal-cyan",
  };

  const bgMap: Record<ToastVariant, string> = {
    success: "bg-terminal-green-glow",
    error: "bg-terminal-red-glow",
    info: "bg-terminal-cyan-glow",
  };

  const Icon = {
    success: CheckCircle,
    error: XCircle,
    info: Info,
  }[t.variant];

  const iconColor: Record<ToastVariant, string> = {
    success: "text-terminal-green",
    error: "text-terminal-red",
    info: "text-terminal-cyan",
  };

  return (
    <motion.div
      initial={{ opacity: 0, x: 40, scale: 0.95 }}
      animate={{ opacity: 1, x: 0, scale: 1 }}
      exit={{ opacity: 0, x: 40, scale: 0.95 }}
      transition={{ duration: 0.2 }}
      className={clsx(
        "pointer-events-auto flex items-center gap-3 px-4 py-3 border border-terminal-border border-l-2 rounded-sm shadow-2xl min-w-72 max-w-sm backdrop-blur-sm",
        accentMap[t.variant],
        bgMap[t.variant]
      )}
    >
      <Icon size={14} className={clsx("flex-shrink-0", iconColor[t.variant])} />
      <span className="flex-1 text-xs font-mono text-terminal-text">{t.message}</span>
      <button
        onClick={() => onDismiss(t.id)}
        className="flex-shrink-0 text-terminal-text-dim hover:text-terminal-text transition-colors"
      >
        <X size={12} />
      </button>
    </motion.div>
  );
}
